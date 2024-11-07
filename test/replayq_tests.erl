-module(replayq_tests).

-include_lib("eunit/include/eunit.hrl").

-define(SUFFIX, "replaylog").
-define(DIR, filename:join([data_dir(), ?FUNCTION_NAME, integer_to_list(uniq())])).

%% the very first run
init_test() ->
  {ok, _} = application:ensure_all_started(replayq),
  Dir = ?DIR,
  Config = #{dir => Dir, seg_bytes => 100},
  Q1 = replayq:open(Config),
  ?assertEqual(0, replayq:count(Q1)),
  ?assertEqual(0, replayq:bytes(Q1)),
  ok = replayq:close(Q1),
  Q2 = replayq:open(Config),
  ?assertEqual(0, replayq:count(Q2)),
  ?assertEqual(0, replayq:bytes(Q2)),
  ok = replayq:close(Q2),
  ok = cleanup(Dir).

reopen_test() ->
  {ok, _} = application:ensure_all_started(replayq),
  Dir = ?DIR,
  Config = #{dir => Dir, seg_bytes => 100},
  Q0 = replayq:open(Config),
  Q1 = replayq:append(Q0, [<<"item1">>, <<"item2">>]),
  ok = replayq:close(Q1),
  Q2 = replayq:open(Config),
  ?assertEqual(2, replayq:count(Q2)),
  ?assertEqual(10, replayq:bytes(Q2)),
  ok = cleanup(Dir).

volatile_test() ->
  Dir = ?DIR,
  Config = #{dir => Dir, seg_bytes => 100},
  Q0 = replayq:open(Config),
  Q1 = replayq:append(Q0, [<<"item1">>, <<"item2">>]),
  ok = replayq:close(Q1),
  Q2 = replayq:open(Config#{offload => {true, volatile}}),
  ?assertEqual(0, replayq:count(Q2)),
  ?assertEqual(0, replayq:bytes(Q2)),
  {Q3, _QAckRef, Items} = replayq:pop(Q2, #{count_limit => 10}),
  ?assertEqual([], Items),
  ok = replayq:close(Q3),
  ok = cleanup(Dir).

%% when popping from in-mem segment, the segment size stats may overflow
%% but not consuming as much memory
offload_in_mem_seg_overflow_test() ->
  {ok, _} = application:ensure_all_started(replayq),
  Dir = ?DIR,
  Config = #{dir => Dir, seg_bytes => 11, offload => true},
  Q0 = replayq:open(Config),
  Q1 = replayq:append(Q0, [<<"item1">>, <<"item2">>]), % in mem, one byte left
  ?assertMatch([], list_segments(Dir)), % not offloading to disk yet
  ?assertMatch(#{w_cur := #{fd := no_fd}}, Q1),
  {Q2, _AckRef1, Items1} = replayq:pop(Q1, #{}),
  ?assertEqual([<<"item1">>, <<"item2">>], Items1),
  Q3 = replayq:append(Q2, [<<"item3">>]), %% still in mem
  ?assertMatch([], list_segments(Dir)), % not offloading to disk yet
  ?assertMatch(#{w_cur := #{fd := no_fd}}, Q3),
  ok = replayq:close(Q3),
  ok = cleanup(Dir).

offload_file_test() ->
  {ok, _} = application:ensure_all_started(replayq),
  Dir = ?DIR,
  Config = #{dir => Dir, seg_bytes => 10, offload => true},
  Q0 = replayq:open(Config),
  Q1 = replayq:append(Q0, [<<"item1">>]), % in mem
  ?assertMatch([], list_segments(Dir)),
  ?assertMatch(#{w_cur := #{fd := no_fd}}, Q1),
  Q2 = replayq:append(Q1, [<<"item2">>]), % in mem, but trigger seg 2 to create
  ?assertEqual([filename(2)], list_segments(Dir)),
  Q3 = replayq:append(Q2, [<<"item3">>, <<"item4">>]), % in seg 2, trigger seg 3 to create
  ?assertEqual([filename(2), filename(3)], list_segments(Dir)),
  {Q4, AckRef1, Items1} = replayq:pop(Q3, #{count_limit => 2}),
  ?assertEqual([<<"item1">>, <<"item2">>], Items1),
  ok = replayq:ack_sync(Q4, AckRef1),
  ?assertEqual([filename(2), filename(3)], list_segments(Dir)),
  {Q5, AckRef2, Items2} = replayq:pop(Q4, #{}),
  ?assertEqual({2, 2}, AckRef2),
  ?assertEqual([<<"item3">>, <<"item4">>], Items2),
  ok = replayq:ack_sync(Q5, AckRef2), %% caught up
  ?assertEqual([filename(2), filename(3)], list_segments(Dir)),
  Q6 = replayq:append(Q5, [<<"item5">>]), % in seg 3
  {Q7, AckRef3, Items3} = replayq:pop(Q6, #{}),
  ?assertEqual([<<"item5">>], Items3),
  ok = replayq:ack_sync(Q7, AckRef3),
  ok = replayq:close(Q7),
  ok = cleanup(Dir).

offload_reopen_test() ->
  {ok, _} = application:ensure_all_started(replayq),
  Dir = ?DIR,
  Config = #{dir => Dir, seg_bytes => 100, offload => true},
  Q0 = replayq:open(Config),
  Q1 = replayq:append(Q0, [<<"item1">>, <<"item2">>]),
  ?assertMatch(#{w_cur := #{fd := no_fd}}, Q1),
  put(noise, noise), % should be filtered out
  {Q2, _AckRef, Items} = replayq:pop(Q1, #{count_limit => 1}),
  ?assertEqual([<<"item1">>], Items),
  ok = replayq:close(Q2),
  Q3 = replayq:open(Config),
  ?assertEqual(2, replayq:count(Q3)),
  ?assertEqual(10, replayq:bytes(Q3)),
  {Q4, AckRef1, Items1} = replayq:pop(Q3, #{count_limit => 2}),
  ?assertEqual([<<"item1">>, <<"item2">>], Items1),
  ?assertEqual([filename(1), filename(2)], list_segments(Dir)),
  ok = replayq:ack_sync(Q4, AckRef1),
  ok = replayq:close(Q4),
  Q5 = replayq:open(Config),
  ?assertEqual(0, replayq:count(Q5)),
  ?assertEqual(0, replayq:bytes(Q5)),
  ok = cleanup(Dir).

reopen_v0_test() ->
  Dir = ?DIR,
  Config = #{dir => Dir, seg_bytes => 1000},
  Q0 = replayq:open(Config),
  #{w_cur := #{fd := Fd}} = Q0, % inspect the opaque internal structure for test
  file:write(Fd, make_v0_iodata(<<"item1">>)), % append a version-0 item
  Q2 = replayq:append(Q0, [<<"item2">>]), % append a version-1 itme
  ok = replayq:close(Q2),
  Q3 = replayq:open(Config),
  {Q4, _AckRef, Items} = replayq:pop(Q3, #{count_limit => 3}),
  %% do not expect item3 because it was appended to a corrupted tail
  ?assertEqual([<<"item1">>, <<"item2">>], Items),
  ?assert(replayq:is_empty(Q4)),
  ok = replayq:close(Q4),
  ok = cleanup(Dir).

append_pop_disk_default_marshaller_test() ->
  {ok, _} = application:ensure_all_started(replayq),
  Dir = ?DIR,
  Config = #{dir => Dir, seg_bytes => 1},
  test_append_pop_disk(Config).

append_pop_disk_my_marshaller_test() ->
  {ok, _} = application:ensure_all_started(replayq),
  Dir = ?DIR,
  Config = #{dir => Dir,
             seg_bytes => 1,
             sizer => fun(Item) -> size(Item) end,
             marshaller => fun(<<"mmp", I/binary>>) -> I;
                              (I) -> <<"mmp", I/binary>>
                           end
            },
  test_append_pop_disk(Config).

test_append_pop_disk(#{dir := Dir} = Config) ->
  {ok, _} = application:ensure_all_started(replayq),
  Q0 = replayq:open(Config),
  Q1 = replayq:append(Q0, [<<"item1">>, <<"item2">>]),
  Q2 = replayq:append(Q1, [<<"item3">>]),
  ?assertEqual(<<"item1">>, replayq:peek(Q2)),
  {Q3, AckRef, Items} = replayq:pop(Q2, #{count_limit => 5,
                                          bytes_limit => 1000}),
  ?assertEqual([<<"item1">>, <<"item2">>, <<"item3">>], Items),
  %% stop without acking
  ok = replayq:close(Q3),
  %% open again expect to receive the same items
  Q4 = replayq:open(Config),
  {Q5, AckRef1, Items1} = replayq:pop(Q4, #{count_limit => 5,
                                            bytes_limit => 1000}),
  ?assertEqual(AckRef, AckRef1),
  ?assertEqual(Items, Items1),
  lists:foreach(fun(_) -> ok = replayq:ack(Q5, AckRef) end, lists:seq(1, 100)),
  ok = replayq:close(Q5),
  Q6 = replayq:open(Config),
  ?assert(replayq:is_empty(Q6)),
  ?assertEqual(empty, replayq:peek(Q6)),
  ?assertEqual({Q6, nothing_to_ack, []}, replayq:pop(Q6, #{})),
  ok = replayq:ack(Q6, nothing_to_ack),
  ok = replayq:close(Q6),
  ok = cleanup(Dir).

append_pop_mem_default_marshaller_test_test() ->
  {ok, _} = application:ensure_all_started(replayq),
  Config = #{mem_only => true},
  test_append_pop_mem(Config).

append_pop_mem_my_marshaller_test_test() ->
  {ok, _} = application:ensure_all_started(replayq),
  Config = #{mem_only => true,
             sizer => fun(Item) -> size(Item) end,
             marshaller => fun(<<"mmp", I/binary>>) -> I;
                              (I) -> <<"mmp", I/binary>>
                           end
            },
  test_append_pop_mem(Config).

test_append_pop_mem(Config) ->
  Q0 = replayq:open(Config),
  Q1 = replayq:append(Q0, [<<"item1">>, <<"item2">>]),
  Q2 = replayq:append(Q1, [<<"item3">>]),
  ?assertEqual(<<"item1">>, replayq:peek(Q2)),
  {Q3, _AckRef, Items} = replayq:pop(Q2, #{count_limit => 5,
                                           bytes_limit => 1000}),
  ?assertEqual([<<"item1">>, <<"item2">>, <<"item3">>], Items),
  %% stop without acking
  ok = replayq:close(Q3),
  %% open again expect to receive the same items
  Q4 = replayq:open(Config),
  {Q5, _AckRef1, _Items1} =
    replayq:pop(Q4, #{count_limit => 5, bytes_limit => 1000}),
  ?assertEqual(empty, replayq:peek(Q5)),
  ?assertEqual({Q5, nothing_to_ack, []}, replayq:pop(Q5, #{})),
  ok = replayq:ack(Q5, nothing_to_ack),
  ok = replayq:close(Q5).

append_max_total_bytes_mem_test() ->
  {ok, _} = application:ensure_all_started(replayq),
  Config = #{mem_only => true,
             sizer => fun(Item) -> size(Item) end,
             marshaller => fun(<<"mmp", I/binary>>) -> I;
                              (I) -> <<"mmp", I/binary>>
                           end,
             max_total_bytes => 10
            },
  test_append_max_total_bytes(Config),
  ok.

append_max_total_bytes_disk_test() ->
  {ok, _} = application:ensure_all_started(replayq),
  Dir = ?DIR,
  Config = #{dir => Dir,
             seg_bytes => 1,
             sizer => fun(Item) -> size(Item) end,
             marshaller => fun(<<"mmp", I/binary>>) -> I;
                              (I) -> <<"mmp", I/binary>>
                           end,
             max_total_bytes => 10
            },
  test_append_max_total_bytes(Config),
  ok = cleanup(Dir).

test_append_max_total_bytes(Config) ->
  {ok, _} = application:ensure_all_started(replayq),
  Q0 = replayq:open(Config),
  ?assertEqual(-10, replayq:overflow(Q0)),
  Q1 = replayq:append(Q0, [<<"item1">>, <<"item2">>, <<"item3">>, <<"item4">>]),
  ?assertEqual(10, replayq:overflow(Q1)),
  {Q2, _AckRef, _Items} = replayq:pop(Q1, #{count_limit => 2}),
  ?assertEqual(0, replayq:overflow(Q2)),
  ok = replayq:close(Q2).

pop_limit_disk_test() ->
  {ok, _} = application:ensure_all_started(replayq),
  Dir = ?DIR,
  Config = #{dir => Dir, seg_bytes => 1},
  ok = test_pop_limit(Config),
  ok = cleanup(Dir).

pop_limit_mem_test() ->
  {ok, _} = application:ensure_all_started(replayq),
  Config = #{mem_only => true},
  ok = test_pop_limit(Config).

test_pop_limit(Config) ->
  Q0 = replayq:open(Config),
  Q1 = replayq:append(Q0, [<<"item1">>, <<"item2">>]),
  Q2 = replayq:append(Q1, [<<"item3">>]),
  {Q3, _AckRef1, Items1} = replayq:pop(Q2, #{count_limit => 1,
                                             bytes_limit => 1000}),
  ?assertEqual([<<"item1">>], Items1),
  {Q4, _AckRef2, Items2} = replayq:pop(Q3, #{count_limit => 10,
                                             bytes_limit => 1}),
  ?assertEqual([<<"item2">>], Items2),
  ok = replayq:close(Q4).

commit_in_the_middle_test() ->
  {ok, _} = application:ensure_all_started(replayq),
  Dir = ?DIR,
  Config = #{dir => Dir, seg_bytes => 1000},
  Q0 = replayq:open(Config),
  Q1 = replayq:append(Q0, [<<"item1">>, <<"item2">>]),
  Q2 = replayq:append(Q1, [<<"item3">>]),
  {Q3, AckRef1, Items1} = replayq:pop(Q2, #{count_limit => 1}),
  ?assertEqual(2, replayq:count(Q3)),
  ?assertEqual(10, replayq:bytes(Q3)),
  timer:sleep(200),
  ok = replayq:ack(Q3, AckRef1),
  ?assertEqual(2, replayq:count(Q3)),
  ?assertEqual([<<"item1">>], Items1),
  ok = replayq:close(Q3),
  Q4 = replayq:open(Config),
  {Q5, _AckRef2, Items2} = replayq:pop(Q4, #{count_limit => 1}),
  ?assertEqual([<<"item2">>], Items2),
  ?assertEqual(1, replayq:count(Q5)),
  ?assertEqual(5, replayq:bytes(Q5)),
  ok = replayq:close(Q5),
  ok = cleanup(Dir).

first_segment_corrupted_test() ->
  {ok, _} = application:ensure_all_started(replayq),
  Dir = ?DIR,
  SegBytes = 10,
  Config = #{dir => Dir, seg_bytes => SegBytes},
  Item = iolist_to_binary(lists:duplicate(SegBytes, "a")),
  Q0 = replayq:open(Config),
  ok = corrupt(Q0),
  Q1 = replayq:append(Q0, [Item]), %% to the first (corrputed) segment
  Q2 = replayq:append(Q1, [Item]), %% to the second segment
  %% assert it has rolled to the 3rd segment
  ?assertMatch(#{head_segno := 1, w_cur := #{segno := 3}}, Q2),
  replayq:close(Q2),
  %% reopen to discover that the first segment is corrupted
  Q3 = replayq:open(Config),
  ?assertEqual(1, replayq:count(Q3)),
  {Q4, _AckRef, Items} = replayq:pop(Q3, #{count_limit => 3}),
  ?assertEqual([Item], Items),
  ?assert(replayq:is_empty(Q4)),
  ok = replayq:close(Q4),
  ok = cleanup(Dir).

second_segment_corrupted_test() ->
  {ok, _} = application:ensure_all_started(replayq),
  Dir = ?DIR,
  SegBytes = 10,
  Config = #{dir => Dir, seg_bytes => SegBytes},
  Item = iolist_to_binary(lists:duplicate(SegBytes, "a")),
  Q0 = replayq:open(Config),
  Q1 = replayq:append(Q0, [Item]), %% to the first segment
  ok = corrupt(Q1),
  Q2 = replayq:append(Q1, [Item]), %% to the second (corrupted) segment
  Q3 = replayq:append(Q2, [Item]), %% to the thrid segment
  %% assert it has rolled to the 4th segment
  ?assertMatch(#{head_segno := 1, w_cur := #{segno := 4}}, Q3),
  replayq:close(Q3),
  %% reopen to discover that the second segment is corrupted
  Q4 = replayq:open(Config),
  ?assertEqual(2, replayq:count(Q4)),
  {Q5, _AckRef, Items} = replayq:pop(Q4, #{count_limit => 3}),
  ?assertEqual([Item, Item], Items),
  ?assert(replayq:is_empty(Q5)),
  ok = replayq:close(Q5),
  ok = cleanup(Dir).

last_segment_corrupted_test() ->
  {ok, _} = application:ensure_all_started(replayq),
  Dir = ?DIR,
  SegBytes = 10,
  Config = #{dir => Dir, seg_bytes => SegBytes},
  Item = iolist_to_binary(lists:duplicate(SegBytes, "a")),
  Q0 = replayq:open(Config),
  Q1 = replayq:append(Q0, [Item]), %% to the first segment
  Q2 = replayq:append(Q1, [Item]), %% to the second segment
  ok = corrupt(Q2),
  Q3 = replayq:append(Q2, [<<"thridsegment">>]), %% to the thrid (corrupted) segment
  replayq:close(Q3),
  %% reopen to discover that the third segment is corrupted
  Q4 = replayq:open(Config),
  ?assertEqual(2, replayq:count(Q4)),
  {Q5, _AckRef, Items} = replayq:pop(Q4, #{count_limit => 3}),
  ?assertEqual([Item, Item], Items),
  LastMsg = <<"yes, can still append">>,
  Q6 = replayq:append(Q5, [LastMsg]),
  ?assertEqual(1, replayq:count(Q6)),
  {Q7, AckRef, [LastMsg]} = replayq:pop(Q6, #{count_limit => 3}),
  ?assert(replayq:is_empty(Q7)),
  replayq:ack(Q7, AckRef),
  ok = replayq:close(Q7),
  %% try to open again to check size
  Q8 = replayq:open(Config),
  ?assert(replayq:is_empty(Q8)),
  replayq:ack(Q7, AckRef),
  ok = cleanup(Dir).

corrupted_segment_test_() ->
  {ok, _} = application:ensure_all_started(replayq),
  [{"ramdom", fun() -> test_corrupted_segment(<<"foo">>) end},
   {"v0-bad-crc", fun() -> test_corrupted_segment(<<0:8, 0:32, 1:32, 1:8>>) end},
   {"v0-zero-crc", fun() -> test_corrupted_segment(<<0:8, 0:32, 0:32, "randomtail">>) end},
   {"v1-non-magic", fun() -> test_corrupted_segment(<<1:8, 0:32, 1:32, 1:8>>) end},
   {"v1-bad-crc-", fun() -> test_corrupted_segment(<<1:8, 841265288:32, 0:32, 1:32, 1:8>>) end}
  ].

test_corrupted_segment(BadBytes) ->
  Dir = ?DIR,
  Config = #{dir => Dir, seg_bytes => 1000},
  Q0 = replayq:open(Config),
  Item2 = <<>>,
  Q1 = replayq:append(Q0, [<<"item1">>, Item2]),
  #{w_cur := #{fd := Fd}} = Q1, % inspect the opaque internal structure for test
  file:write(Fd, BadBytes), % corrupt the file
  Q2 = replayq:append(Q0, [<<"item3">>]),
  ok = replayq:close(Q2),
  Q3 = replayq:open(Config),
  {Q4, _AckRef, Items} = replayq:pop(Q3, #{count_limit => 3}),
  %% do not expect item3 because it was appended to a corrupted tail
  ?assertEqual([<<"item1">>, Item2], Items),
  ?assert(replayq:is_empty(Q4)),
  ok = replayq:close(Q4),
  ok = cleanup(Dir).

comitter_crash_test() ->
  {ok, _} = application:ensure_all_started(replayq),
  Dir = ?DIR,
  Config = #{dir => Dir, seg_bytes => 1000},
  #{committer := Committer} = replayq:open(Config),
  erlang:process_flag(trap_exit, true),
  Committer ! <<"foo">>,
  receive
    {'EXIT', _Pid, {replayq_committer_unkown_msg, <<"foo">>}} ->
      ok
  end.

%% Checks that our spawned committer can register a name for itself when using filepaths
%% larger than 255 bytes.
huge_filepath_test() ->
    {ok, _} = application:ensure_all_started(replayq),
    Dir0 = ?DIR,
    Dir = filename:join(Dir0, binary:copy(<<"a">>, 255)),
    Config = #{dir => Dir, seg_bytes => 1000},
    Q = #{committer := Committer} = replayq:open(Config),
    ?assert(is_process_alive(Committer)),
    replayq:close(Q),
    ok.

%% Checks that we don't allow having the same directory open by multiple replayqs.
same_directory_committer_clash_test() ->
    {ok, _} = application:ensure_all_started(replayq),
    Dir = ?DIR,
    Config = #{dir => Dir, seg_bytes => 1000},
    Q1 = replayq:open(Config),
    try replayq:open(Config) of
        Q2 -> error({"should not allow opening a second replayq", Q2})
    catch
        error:{badmatch, {error, already_registered}} ->
            ok
    end,
    replayq:close(Q1),
    ok.

is_in_mem_test_() ->
  {ok, _} = application:ensure_all_started(replayq),
  [ {"mem queue", fun() ->
                      Q = replayq:open(#{mem_only => true}),
                      true = replayq:is_mem_only(Q),
                      ok = replayq:close(Q)
                  end}
  , {"disk queue", fun() ->
                       Config = #{dir => ?DIR, seg_bytes => 100},
                       Q = replayq:open(Config),
                       false = replayq:is_mem_only(Q),
                       ok = replayq:close(Q)
                   end}
  ].

stop_before_test_() ->
  {ok, _} = application:ensure_all_started(replayq),
  [{"mem queue",
    fun() ->
            Config = #{mem_only => true},
            stop_before_test(Config),
            stop_before_readme_example_test(Config)
    end},
   {"disk queue",
    fun() ->
            Config1 = #{
                       dir => ?DIR,
                       seg_bytes => 100
                      },
            stop_before_test(Config1),
            Config2 = #{
                       dir => filename:join([?DIR, "example"]),
                       seg_bytes => 100
                      },
            stop_before_readme_example_test(Config2)
    end}].

stop_before_test(Config) ->
  {ok, _} = application:ensure_all_started(replayq),
    Q0 = replayq:open(Config),
    Q1 = replayq:append(Q0, [<<"1">>, <<"2">>, <<"3">>, <<"4">>, <<"5">>]),
    StopBeforeFun =
      fun(_Item, #{stop_ctr := 3}) ->
           true;
         (_Item, #{stop_ctr := Cnt}) ->
           #{stop_ctr => Cnt + 1}
      end,
    {Q2, AckRef, Items} =
      replayq:pop(Q1,
                  #{
                    count_limit => 100,
                    bytes_limit => 10000000,
                    stop_before => {StopBeforeFun, #{stop_ctr => 0}}
                   }),
    ok = replayq:ack(Q2, AckRef),
    ?assertEqual([<<"1">>, <<"2">>, <<"3">>], Items),
    ?assertEqual(2, replayq:count(Q2)),
    ok = replayq:close(Q2).

%% Test that the example in the readme file works
stop_before_readme_example_test(Config) ->
    {ok, _} = application:ensure_all_started(replayq),
    Q0 = replayq:open(Config),
    Q1 = replayq:append(Q0,
                        [
                         <<"type1">>,
                         <<"type1">>,
                         <<"type2">>,
                         <<"type2">>,
                         <<"type2">>,
                         <<"type3">>
                        ]),
    StopBeforeFunc =
    fun(Item, #{current_type := none}) ->
            #{current_type => Item};
       (Item, #{current_type := Item}) ->
            %% Item and current_type are the same
            #{current_type => Item};
       (_Item, #{current_type := _OtherType}) ->
            %% Return true to stop collecting items before the current item
            true
    end,
    StopBeforeInitialState = #{current_type => none},
    StopBefore = {StopBeforeFunc, StopBeforeInitialState},
    %% We will stop because the stop_before function returns true
    {Q2, AckRef1, [<<"type1">>, <<"type1">>]} =
        replayq:pop(Q1, #{stop_before => StopBefore, count_limit => 10}),
    ok = replayq:ack(Q2, AckRef1),
    %% We will stop because of the count_limit
    {Q3, AckRef2, [<<"type2">>]} =
        replayq:pop(Q2, #{stop_before => StopBefore, count_limit => 1}),
    ok = replayq:ack(Q3, AckRef2),
    %% We will stop because the stop_before function returns true
    {Q4, AckRef3, [<<"type2">>, <<"type2">>]} =
        replayq:pop(Q3, #{stop_before => StopBefore, count_limit => 10}),
    ok = replayq:ack(Q4, AckRef3),
    %% We will stop because the queue gets empty
    {Q5, AckRef4, [<<"type3">>]} =
        replayq:pop(Q4, #{stop_before => StopBefore, count_limit => 10}),
    ok = replayq:ack(Q5, AckRef4),
    ok = replayq:close(Q5).

corrupted_commit_test() ->
  Dir = ?DIR,
  Config = #{dir => Dir, seg_bytes => 1000},
  Q0 = replayq:open(Config),
  Q1 = replayq:append(Q0, [<<"item1">>]),
  {Q2, AckRef, _} = replayq:pop(Q1, #{count_limit => 3}),
  ok = replayq:ack_sync(Q2, AckRef),
  ok = replayq:close(Q2),
  CommitFile = filename:join(Dir, "COMMIT"),
  ?assertMatch({ok, [_]}, file:consult(CommitFile)),

  ok = file:write_file(CommitFile, <<>>),
  %% assert no crash
  Q3 = replayq:open(Config),
  ok = replayq:close(Q3),

  ok = file:write_file(CommitFile, <<"bad-erlang-term">>),
  %% assert no crash
  Q4 = replayq:open(Config),
  ok = replayq:close(Q4),
  ok = cleanup(Dir).

%% helpers ===========================================================

cleanup(Dir) ->
  Files = list_segments(Dir),
  ok = lists:foreach(fun(F) -> ok = file:delete(filename:join(Dir, F)) end, Files),
  _ = file:delete(filename:join(Dir, "COMMIT")),
  ok = file:del_dir(Dir).

list_segments(Dir) -> filelib:wildcard("*."?SUFFIX, Dir).

data_dir() -> "./test-data".

uniq() ->
  {_, _, Micro} = erlang:timestamp(),
  Micro.

make_v0_iodata(Item) ->
  Size = size(Item),
  CRC = erlang:crc32(Item),
  [<<0:8, CRC:32/unsigned-integer, Size:32/unsigned-integer>>, Item].

filename(Segno) ->
  lists:flatten(io_lib:format("~10.10.0w."?SUFFIX, [Segno])).

%% corrupt the segment
corrupt(#{w_cur := #{fd := Fd}}) ->
  file:write(Fd, "some random bytes").
