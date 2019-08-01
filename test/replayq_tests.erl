-module(replayq_tests).

-include_lib("eunit/include/eunit.hrl").

-define(SUFFIX, "replaylog").
-define(DIR, filename:join([data_dir(), ?FUNCTION_NAME, integer_to_list(uniq())])).

%% the very first run
init_test() ->
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
  Dir = ?DIR,
  Config = #{dir => Dir, seg_bytes => 100},
  Q0 = replayq:open(Config),
  Q1 = replayq:append(Q0, [<<"item1">>, <<"item2">>]),
  ok = replayq:close(Q1),
  Q2 = replayq:open(Config),
  ?assertEqual(2, replayq:count(Q2)),
  ?assertEqual(10, replayq:bytes(Q2)),
  ok = cleanup(Dir).


append_pop_disk_default_marshaller_test() ->
  Dir = ?DIR,
  Config = #{dir => Dir, seg_bytes => 1},
  test_append_pop_disk(Config).

append_pop_disk_my_marshaller_test() ->
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
  ok = replayq:ack(Q5, AckRef),
  ok = replayq:close(Q5),
  Q6 = replayq:open(Config),
  ?assert(replayq:is_empty(Q6)),
  ?assertEqual(empty, replayq:peek(Q6)),
  ?assertEqual({Q6, nothing_to_ack, []}, replayq:pop(Q6, #{})),
  ok = replayq:ack(Q6, nothing_to_ack),
  ok = replayq:close(Q6),
  ok = cleanup(Dir).

append_pop_mem_default_marshaller_test_test() ->
  Config = #{mem_only => true},
  test_append_pop_mem(Config).

append_pop_mem_my_marshaller_test_test() ->
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

append_limit_mem_test() ->
  Config = #{mem_only => true,
             sizer => fun(Item) -> size(Item) end,
             marshaller => fun(<<"mmp", I/binary>>) -> I;
                              (I) -> <<"mmp", I/binary>>
                           end,
             limit_size => 10
            },
  Q0 = replayq:open(Config),
  Q1 = replayq:append(Q0, [<<"item1">>, <<"item2">>, <<"item3">>, <<"item4">>]),
  {Q2, _AckRef, Items} = replayq:pop(Q1, #{count_limit => 1}),
  ?assertEqual([<<"item3">>], Items),
  replayq:close(Q2),
  ok.

append_limit_disk_test() ->
  Dir = ?DIR,
  Config = #{dir => Dir,
             seg_bytes => 1,
             sizer => fun(Item) -> size(Item) end,
             marshaller => fun(<<"mmp", I/binary>>) -> I;
                              (I) -> <<"mmp", I/binary>>
                           end,
             limit_size => 10
            },
  Q0 = replayq:open(Config),
  Q1 = replayq:append(Q0, [<<"item1">>, <<"item2">>, <<"item3">>, <<"item4">>]),
  {Q2, _AckRef, Items} = replayq:pop(Q1, #{count_limit => 1}),
  ?assertEqual([<<"item3">>], Items),
  replayq:close(Q2),
  ok.

pop_limit_disk_test() ->
  Dir = ?DIR,
  Config = #{dir => Dir, seg_bytes => 1},
  ok = test_pop_limit(Config),
  ok = cleanup(Dir).

pop_limit_mem_test() ->
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

corrupted_segment_test() ->
  %% some random injection
  ok = test_corrupted_segment(<<"foo">>),
  %% a bad CRC
  ok = test_corrupted_segment(<<0:8, 0:32, 1:32, 1:8>>).

test_corrupted_segment(BadBytes) ->
  Dir = ?DIR,
  Config = #{dir => Dir, seg_bytes => 1000},
  Q0 = replayq:open(Config),
  Q1 = replayq:append(Q0, [<<"item1">>, <<>>]),
  #{w_cur := #{fd := Fd}} = Q1, % inspect the opaque internal structure for test
  file:write(Fd, BadBytes), % corrupt the file
  Q2 = replayq:append(Q0, [<<"item3">>]),
  ok = replayq:close(Q2),
  Q3 = replayq:open(Config),
  {Q4, _AckRef, Items} = replayq:pop(Q3, #{count_limit => 3}),
  %% do not expect item3 because it was appened to a corrupted tail
  ?assertEqual([<<"item1">>, <<>>], Items),
  ?assert(replayq:is_empty(Q4)),
  ok = replayq:close(Q4),
  ok = cleanup(Dir).

comitter_crash_test() ->
  Dir = ?DIR,
  ComitterName = binary_to_atom(iolist_to_binary(filename:join([Dir, committer])), utf8),
  Config = #{dir => Dir, seg_bytes => 1000},
  _ = replayq:open(Config),
  erlang:process_flag(trap_exit, true),
  ComitterName ! <<"foo">>,
  receive
    {'EXIT', _Pid, {replayq_committer_unkown_msg, <<"foo">>}} ->
      ok
  end.

%% helpers ===========================================================

cleanup(Dir) ->
  Files = filelib:wildcard("*."?SUFFIX, Dir),
  ok = lists:foreach(fun(F) -> ok = file:delete(filename:join(Dir, F)) end, Files),
  _ = file:delete(filename:join(Dir, "COMMIT")),
  ok = file:del_dir(Dir).

data_dir() -> "./test-data".

uniq() ->
  {_, _, Micro} = erlang:timestamp(),
  Micro.

