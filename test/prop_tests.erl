-module(prop_tests).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

run_persistent_test_() ->
  Opts = [{numtests, 1000}, {to_file, user}],
  {timeout, 60,
   fun() -> ?assert(proper:quickcheck(prop_run(false), Opts)) end}.

run_offload_test_() ->
  Opts = [{numtests, 1000}, {to_file, user}],
  {timeout, 60,
   fun() -> ?assert(proper:quickcheck(prop_run(true), Opts)) end}.


prop_run(IsOffload) ->
  ?FORALL({SegBytes, OpList},
          {prop_seg_bytes(), prop_op_list(IsOffload)},
          begin
            Dir = filename:join([data_dir(), integer_to_list(erlang:system_time())]),
            MQ = replayq:open(#{mem_only => true}),
            Cfg = #{dir => Dir, seg_bytes => SegBytes, offload => IsOffload},
            DQ = replayq:open(Cfg),
            try
              ok = apply_ops(MQ, DQ, OpList, Cfg),
              true
            after
              replayq:close(DQ),
              ok = delete_dir(Dir)
            end
          end).

apply_ops(MQ, DQ, [], _) ->
  ok = compare(MQ, DQ);
apply_ops(MQ0, DQ0, [Op | Rest], Cfg) ->
  {MQ, DQ} = apply_op(MQ0, DQ0, Op, Cfg),
  ok = compare_stats(MQ, DQ),
  apply_ops(MQ, DQ, Rest, Cfg).

apply_op(MQ0, DQ0, {append, Items}, _Cfg)->
  {replayq:append(MQ0, Items),
   replayq:append(DQ0, Items)};
apply_op(MQ0, DQ0, {pop_ack, {Bytes, Count}}, _Cfg) ->
  Opts = #{bytes_limit => Bytes, count_limit => Count},
  {MQ, AckRef1, Items1} = replayq:pop(MQ0, Opts),
  {DQ, AckRef2, Items2} = replayq:pop(DQ0, Opts),
  ?assertEqual(Items1, Items2),
  ok = replayq:ack_sync(MQ, AckRef1),
  ok = replayq:ack_sync(DQ, AckRef2),
  {MQ, DQ};
apply_op(MQ, DQ0, reopen, Cfg) ->
  ok = replayq:close(MQ),
  ok = replayq:close(DQ0),
  DQ = replayq:open(Cfg),
  {MQ, DQ}.

data_dir() -> filename:join(["./test-data", "prop-tests"]).

prop_seg_bytes() -> proper_types:integer(100, 1000).

prop_items() ->
  proper_types:list(proper_types:binary()).

prop_pop_args() ->
  {_Bytes = proper_types:integer(1,1000), _Count = proper_types:integer(1,10)}.

prop_op_list(IsOffload) ->
  Base = [{append, prop_items()}, {pop_ack, prop_pop_args()}],
  Union = case IsOffload of
            true -> Base; %% can not support reopen in proptest
            false -> [reopen | Base]
          end,
  proper_types:list(proper_types:oneof(Union)).

delete_dir(Dir) ->
  lists:foreach(fun(F) -> ok = file:delete(filename:join([Dir, F])) end,
                filelib:wildcard("*", Dir)),
  ok = file:del_dir(Dir).

compare_stats(MQ, DQ) ->
  ?assertEqual(replayq:count(MQ), replayq:count(DQ)),
  ?assertEqual(replayq:bytes(MQ), replayq:bytes(DQ)),
  ok.

compare(Q1, Q2) ->
  {NewQ1, _, Items1} = replayq:pop(Q1, #{count_limit => 1}),
  {NewQ2, _, Items2} = replayq:pop(Q2, #{count_limit => 1}),
  case Items1 =:= Items2 of
    true when Items1 =:= [] -> ok; %% done
    true -> compare(NewQ1, NewQ2);
    false -> throw({diff, {Items1, NewQ1}, {Items2, NewQ2}})
  end.

