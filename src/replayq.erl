-module(replayq).

-export([open/1, close/1]).
-export([append/2, pop/2, ack/2, peek/1]).
-export([count/1, bytes/1, is_empty/1]).

-export([committer_loop/2]).

-export_type([config/0, q/0, ack_ref/0]).

-define(NOTHING_TO_ACK, nothing_to_ack).

-type segno() :: pos_integer().
-type item() :: binary().
-type count() :: non_neg_integer().
-type bytes() :: non_neg_integer().
-type filename() :: file:filename_all().
-type dir() :: filename().
-type ack_ref() :: ?NOTHING_TO_ACK | {segno(), ID :: pos_integer()}.

-type config() :: #{dir => dir(),
                    seg_bytes => bytes(),
                    mem_only => boolean()
                   }.
%% writer cursor
-type w_cur() :: #{segno := segno(),
                   bytes := bytes(),
                   count := count(),
                   fd := file:fd()
                  }.

-type stats() :: #{bytes := bytes(),
                   count := count()
                  }.

-opaque q() :: #{config := config(),
                 stats := stats(),
                 in_mem := queue:queue(item()),
                 w_cur => w_cur(),
                 committer =>  pid(),
                 head_segno => segno()
                }.

-define(LAYOUT_VSN, 0).
-define(SUFFIX, "replaylog").
-define(DEFAULT_POP_BYTES_LIMIT, 2000000).
-define(DEFAULT_POP_COUNT_LIMIT, 1000).
-define(COMMIT(SEGNO, ID), {commit, SEGNO, ID}).
-define(NO_COMMIT_HIST, no_commit_hist).
-define(FIRST_SEGNO, 1).
-define(NEXT_SEGNO(N), (N + 1)).
-define(STOP, stop).

-spec open(config()) -> q().
open(#{mem_only := true} = Config) ->
  #{config => mem_only,
    stats => #{bytes => 0, count => 0},
    in_mem => queue:new()
   };
open(#{dir := Dir, seg_bytes := _} = Config) ->
  ok = filelib:ensure_dir(filename:join(Dir, "foo")),
  case delete_consumed_and_list_rest(Dir) of
    [] ->
      %% no old segments
      #{config => Config,
        stats => #{bytes => 0, count => 0},
        w_cur => open_segment(Dir, ?FIRST_SEGNO),
        committer => spawn_committer(?FIRST_SEGNO, Dir),
        head_segno => ?FIRST_SEGNO,
        in_mem => queue:new()
       };
    Segs ->
      LastSegno = lists:last(Segs),
      CommitHist = get_commit_hist(Dir),
      HeadItems = read_items(Dir, hd(Segs), CommitHist),
      #{config => Config,
        stats => collect_stats(Dir, HeadItems, tl(Segs)),
        w_cur => maybe_roll_out_to_new_seg(Dir, LastSegno),
        committer => spawn_committer(hd(Segs), Dir),
        head_segno => hd(Segs),
        in_mem => queue:from_list(HeadItems)
       }
  end.

-spec close(q() | w_cur()) -> ok.
close(#{config := mem_only}) -> ok;
close(#{w_cur := W_Cur, committer := Pid}) ->
  MRef = erlang:monitor(process, Pid),
  Pid ! ?STOP,
  unlink(Pid),
  receive
    {'DOWN', MRef, process, Pid, _Reason} ->
      ok
  end,
  close(W_Cur);
close(#{fd := Fd}) ->
  ok = file:close(Fd).

-spec append(q(), [item()]) -> q().
append(#{config := mem_only,
         in_mem := InMem,
         stats := #{bytes := Bytes0, count := Count0}
        } = Q, Items) ->
  Count1 = erlang:length(Items),
  Bytes1 = erlang:iolist_size(Items),
  Stats = #{count => Count0 + Count1, bytes => Bytes0 + Bytes1},
  Q#{stats := Stats,
     in_mem := queue:join(InMem, queue:from_list(Items))
    };
append(#{config := #{seg_bytes := BytesLimit, dir := Dir},
         stats := #{bytes := Bytes0, count := Count0},
         w_cur := #{segno := WriterSegno, count := CountInSeg} = W_Cur0,
         head_segno := HeadSegno,
         in_mem := HeadItems0
        } = Q, Items0) ->
  Items = assign_id(CountInSeg + 1, Items0),
  IoData = lists:map(fun make_iodata/1, Items),
  Count1 = erlang:length(Items),
  Bytes1 = erlang:iolist_size(Items0),
  Stats = #{count => Count0 + Count1, bytes => Bytes0 + Bytes1},
  W_Cur =
    case do_append(W_Cur0, Count1, Bytes1, IoData) of
      #{segno := Segno, bytes := Bytes} = W_Cur1 when Bytes >= BytesLimit ->
        %% here we check if segment size is greater than segment size limit
        %% after append based on the assumption that the items are usually
        %% very small in size comparing to segment size.
        %% We can change implementation to split items list to avoid
        %% segment overflow if really necessary
        close(W_Cur1),
        open_segment(Dir, ?NEXT_SEGNO(Segno));
      W_Cur1 ->
        W_Cur1
    end,
  HeadItems =
    case HeadSegno =:= WriterSegno of
      true -> queue:join(HeadItems0, queue:from_list(Items));
      false -> HeadItems0
    end,
  Q#{stats := Stats,
     w_cur := W_Cur,
     in_mem := HeadItems
    }.

%% @doc pop out at least one item from the queue.
%% volume limitted by `bytes_limit' and `count_limit'.
-spec pop(q(), #{bytes_limit => bytes(), count_limit => count()}) ->
        {q(), ack_ref(), [item()]}.
pop(Q, Opts) ->
  Bytes = maps:get(bytes_limit, Opts, ?DEFAULT_POP_BYTES_LIMIT),
  Count = maps:get(count_limit, Opts, ?DEFAULT_POP_COUNT_LIMIT),
  true = (Count > 0),
  pop(Q, Bytes, Count, ?NOTHING_TO_ACK, []).

%% @doc peek the queue front item.
-spec peek(q()) -> empty | {value, item()}.
peek(#{in_mem := HeadItems}) ->
  queue:peek(HeadItems).

%% @doc Asynch-ly write the consumed item Segment number + ID to a file.
-spec ack(q(), ack_ref()) -> ok.
ack(_, ?NOTHING_TO_ACK) -> ok;
ack(#{committer := Pid}, {Segno, Id}) ->
  Pid ! ?COMMIT(Segno, Id),
  ok.

count(#{stats := #{count := Count}}) -> Count.

bytes(#{stats := #{bytes := Bytes}}) -> Bytes.

is_empty(#{config := mem_only, in_mem := All}) ->
  queue:is_empty(All);
is_empty(#{w_cur := #{segno := TailSegno},
           head_segno := HeadSegno,
           in_mem := HeadItems
          } = Q) ->
  Result = ((TailSegno =:= HeadSegno) andalso queue:is_empty(HeadItems)),
  Result = (count(Q) =:= 0). %% assert

%% internals =========================================================

pop(Q, _Bytes, 0, AckRef, Acc) ->
  {Q, AckRef, lists:reverse(Acc)};
pop(#{config := Cfg} = Q, Bytes, Count, AckRef, Acc) ->
  case is_empty(Q) of
    true ->
      {Q, AckRef, lists:reverse(Acc)};
    false when Cfg =:= mem_only ->
      pop_mem(Q, Bytes, Count, Acc);
    false ->
      pop2(Q, Bytes, Count, AckRef, Acc)
  end.

pop_mem(#{in_mem := InMem,
          stats := #{count := TotalCount, bytes := TotalBytes} = Stats
         } = Q, Bytes, Count, Acc) ->
  case queue:out(InMem) of
    {{value, Item}, _} when size(Item) > Bytes andalso Acc =/= [] ->
      {Q, ?NOTHING_TO_ACK, lists:reverse(Acc)};
    {{value, Item}, Rest} ->
      NewQ = Q#{in_mem := Rest,
                stats := Stats#{count := TotalCount - 1,
                                bytes := TotalBytes - size(Item)
                               }
               },
      pop(NewQ, Bytes - size(Item), Count - 1, ?NOTHING_TO_ACK, [Item | Acc])
  end.

pop2(#{head_segno := HeadSegno,
       in_mem := HeadItems,
       stats := #{count := TotalCount, bytes := TotalBytes} = Stats,
       w_cur := #{segno := WriterSegno}
      } = Q, Bytes, Count, AckRef, Acc) ->
  case queue:out(HeadItems) of
    {{value, {_Id, Item}}, _} when size(Item) > Bytes andalso Acc =/= [] ->
      %% taking the head item would cause exceeding size limit
      {Q, AckRef, lists:reverse(Acc)};
    {{value, {Id, Item}}, HeadItemsX} ->
      Q1 = Q#{in_mem := HeadItemsX,
              stats := Stats#{count := TotalCount - 1,
                              bytes := TotalBytes - size(Item)
                             }
             },
      %% read the next segment in case current is drained
      NewQ = case queue:is_empty(HeadItemsX) andalso HeadSegno < WriterSegno of
               true -> read_next_seg(Q1);
               false -> Q1
             end,
      NewAckRef = {HeadSegno, Id},
      pop(NewQ, Bytes - size(Item), Count - 1, NewAckRef, [Item | Acc])
  end.

read_next_seg(#{config := #{dir := Dir}, head_segno := HeadSegno} = Q) ->
  Q#{head_segno := HeadSegno + 1,
     in_mem := queue:from_list(read_items(Dir, HeadSegno + 1, ?NO_COMMIT_HIST))
    }.

delete_consumed_and_list_rest(Dir0) ->
  Dir = unicode:characters_to_list(Dir0),
  Segnos0 = lists:sort([parse_segno(N) || N <- filelib:wildcard("*."?SUFFIX, Dir)]),
  {SegnosToDelete, Segnos} = find_segnos_to_delete(Dir, Segnos0, get_commit_hist(Dir)),
  ok = lists:foreach(fun(Segno) -> ensure_deleted(filename(Dir, Segno)) end, SegnosToDelete),
  case Segnos of
    [] ->
      %% delete commit file in case there is no segments left
      %% segment number will start from 0 again.
      ensure_deleted(commit_filename(Dir)),
      [];
    X ->
      X
  end.

find_segnos_to_delete(_Dir, Segnos, ?NO_COMMIT_HIST) -> {[], Segnos};
find_segnos_to_delete(Dir, Segnos0, {CommittedSegno, CommittedId}) ->
  {SegnosToDelete, Segnos} = lists:partition(fun(N) -> N < CommittedSegno end, Segnos0),
  case Segnos =/= [] andalso
       hd(Segnos) =:= CommittedSegno andalso
       is_all_consumed(Dir, CommittedSegno, CommittedId) of
    true ->
      CommittedSegno = hd(Segnos), %% assert
      %% all items in the oldest segment have been consumed,
      %% no need to keep this segment
      {[CommittedSegno | SegnosToDelete], tl(Segnos)};
    _ ->
      {SegnosToDelete, Segnos}
  end.

%% ALL items are consumed if the committed item ID is no-less than the number
%% of items in this segment
is_all_consumed(Dir, CommittedSegno, CommittedId) ->
  CommittedId >= erlang:length(do_read_items(Dir, CommittedSegno)).

ensure_deleted(Filename) ->
  case file:delete(Filename) of
    ok -> ok;
    {error, enoent} -> ok
  end.

%% The committer writes consumer's acked segmeng number + item ID
%% to a file. The file is only read at start/restart.
spawn_committer(HeadSegno, Dir) ->
  Name = iolist_to_binary(filename:join([Dir, committer])),
  %% register a name to avoid having two committers spawned for the same dir
  RegName = binary_to_atom(Name, utf8),
  erlang:spawn_link(fun() ->
                        true = erlang:register(RegName, self()),
                        committer_loop(HeadSegno, Dir)
                    end).

committer_loop(HeadSegno, Dir) ->
  receive
    ?COMMIT(Segno, Id) ->
      IoData = io_lib:format("~p.\n", [#{segno => Segno, id => Id}]),
      ok = file:write_file(commit_filename(Dir), IoData),
      case Segno > HeadSegno of
        true ->
          SegnosToDelete = lists:seq(HeadSegno, Segno - 1),
          lists:foreach(fun(N) -> ok = ensure_deleted(filename(Dir, N)) end, SegnosToDelete);
        false ->
          ok
      end,
      ?MODULE:committer_loop(Segno, Dir);
    ?STOP ->
      ok
  after
    200 ->
      ?MODULE:committer_loop(HeadSegno, Dir)
  end.

get_commit_hist(Dir) ->
  CommitFile = commit_filename(Dir),
  case filelib:is_regular(CommitFile) of
    true ->
      {ok, [#{segno := Segno, id := Id}]} = file:consult(CommitFile),
      {Segno, Id};
    false ->
      ?NO_COMMIT_HIST
  end.

commit_filename(Dir) ->
  filename:join([Dir, "COMMIT"]).

%% Assign a per-segment sequence number (1-based) for each item within a segment
%% This ID is (together with segment number) used to acknowledge consumed items.
assign_id(_Id, []) -> [];
assign_id(Id, [Item | Rest]) -> [{Id, Item} | assign_id(Id + 1, Rest)].

do_append(#{fd := Fd, bytes := Bytes0, count := Count0} = Cur,
          Count, Bytes, IoData) ->
  ok = file:write(Fd, IoData),
  Cur#{bytes => Bytes0 + Bytes,
       count => Count0 + Count
      }.

read_items(Dir, Segno, CommitHist) ->
  Items = do_read_items(Dir, Segno),
  case CommitHist of
    ?NO_COMMIT_HIST -> Items;
    {Segno, CommittedId} -> [{I, Item} || {I, Item} <- Items, I > CommittedId];
    {CommitedSegno, _} when CommitedSegno < Segno -> Items
  end.

do_read_items(Dir, Segno) ->
  Filename = filename(Dir, Segno),
  {ok, Bin} = file:read_file(Filename),
  case parse_items(Bin, 1, []) of
    {Items, <<>>} ->
      Items;
    {Items, Corrupted} ->
      error_logger:error_msg("corrupted replayq log: ~s, skipped ~p bytes",
                             [Filename, size(Corrupted)]),
      Items
  end.

parse_items(<<>>, _Id, Acc) -> {lists:reverse(Acc), <<>>};
parse_items(<<?LAYOUT_VSN:8, CRC:32/unsigned-integer, Size:32/unsigned-integer,
              Item:Size/binary, Rest/binary>> = All, Id, Acc) ->
  case CRC =:= erlang:crc32(Item) of
    true -> parse_items(Rest, Id + 1, [{Id, Item} | Acc]);
    false -> {lists:reverse(Acc), All}
  end;
parse_items(Corrupted, _Id, Acc) ->
  {lists:reverse(Acc), Corrupted}.

make_iodata({_Id, Item}) when is_binary(Item) ->
  Size = size(Item),
  CRC = erlang:crc32(Item),
  [<<?LAYOUT_VSN:8, CRC:32/unsigned-integer, Size:32/unsigned-integer>>, Item].

collect_stats(Dir, HeadItems, SegsOnDisk) ->
  ItemF = fun({_, Item}, {B, C}) -> {B + size(Item), C + 1} end,
  Acc0 = lists:foldl(ItemF, {0, 0}, HeadItems),
  {Bytes, Count} =
    lists:foldl(fun(Segno, Acc) ->
                    Items = read_items(Dir, Segno, ?NO_COMMIT_HIST),
                    lists:foldl(ItemF, Acc, Items)
                end, Acc0, SegsOnDisk),
  #{bytes => Bytes, count => Count}.

parse_segno(Filename) ->
  [Segno, ?SUFFIX] = string:tokens(Filename, "."),
  list_to_integer(Segno).

filename(Dir, Segno) ->
  Name = lists:flatten(io_lib:format("~10.10.0w."?SUFFIX, [Segno])),
  filename:join(Dir, Name).

%% open the current segment for write if it is empty
%% otherwise rollout to the next segment
-spec maybe_roll_out_to_new_seg(dir(), segno()) -> w_cur().
maybe_roll_out_to_new_seg(Dir, Segno) ->
  Filename = filename(Dir, Segno),
  case filelib:file_size(Filename) of
    0 -> open_segment(Dir, Segno);
    _ -> open_segment(Dir, ?NEXT_SEGNO(Segno))
  end.

-spec open_segment(dir(), segno()) -> w_cur().
open_segment(Dir, Segno) ->
  Filename = filename(Dir, Segno),
  %% raw so there is no need to go through the single gen_server file_server
  {ok, Fd} = file:open(Filename, [raw, read, write, binary, delayed_write]),
  #{fd => Fd, segno => Segno, bytes => 0, count => 0}.

