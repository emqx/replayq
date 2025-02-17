%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(replayq_proper_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [{group, queue}, {group, ets_exclusive}].

groups() ->
    [
        {queue, [], all_cases()},
        {ets_exclusive, [], all_cases()}
    ].

all_cases() ->
    [
        F
     || {F, _} <- ?MODULE:module_info(exports),
        is_t_function(atom_to_list(F))
    ].

is_t_function("t_" ++ _) -> true;
is_t_function(_) -> false.

init_per_group(Group, Config) ->
    [{ct_group, Group} | Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(replayq),
    Config.

end_per_suite(_Config) ->
    ok.

get_mem_queue_module(Config) ->
    case proplists:get_value(ct_group, Config) of
        queue -> replayq_mem_queue;
        ets_exclusive -> replayq_mem_ets_exclusive
    end.

t_run_persistent(Config) ->
    MemQueueModule = get_mem_queue_module(Config),
    Opts = [{numtests, 1000}, {to_file, user}],
    true = proper:quickcheck(prop_run(MemQueueModule, false), Opts).

t_run_offload(Config) ->
    MemQueueModule = get_mem_queue_module(Config),
    Opts = [{numtests, 1000}, {to_file, user}],
    true = proper:quickcheck(prop_run(MemQueueModule, true), Opts).

prop_run(MemQueueModule, IsOffload) ->
    ?FORALL(
        {SegBytes, OpList},
        {prop_seg_bytes(), prop_op_list(IsOffload)},
        begin
            Dir = filename:join([data_dir(), integer_to_list(erlang:system_time())]),
            MQCfg = #{mem_only => true, mem_queue_module => MemQueueModule},
            MQ = replayq:open(MQCfg),
            DQCfg = #{
                dir => Dir,
                seg_bytes => SegBytes,
                offload => IsOffload,
                mem_queue_module => MemQueueModule
            },
            DQ = replayq:open(DQCfg),
            try
                ok = apply_ops(MQ, DQ, OpList, MQCfg, DQCfg),
                true
            after
                replayq:close(DQ),
                ok = delete_dir(Dir)
            end
        end
    ).

apply_ops(MQ, DQ, [], _MQCfg, _DQCfg) ->
    ok = compare(MQ, DQ);
apply_ops(MQ0, DQ0, [Op | Rest], MQCfg, DQCfg) ->
    {MQ, DQ} = apply_op(MQ0, DQ0, Op, MQCfg, DQCfg),
    ok = compare_stats(MQ, DQ),
    apply_ops(MQ, DQ, Rest, MQCfg, DQCfg).

apply_op(MQ0, DQ0, {append, Items}, _MQCfg, _DQCfg) ->
    {replayq:append(MQ0, Items), replayq:append(DQ0, Items)};
apply_op(MQ0, DQ0, {pop_ack, {Bytes, Count}}, _MQCfg, _DQCfg) ->
    Opts = #{bytes_limit => Bytes, count_limit => Count},
    {MQ, AckRef1, Items1} = replayq:pop(MQ0, Opts),
    {DQ, AckRef2, Items2} = replayq:pop(DQ0, Opts),
    ?assertEqual(Items1, Items2),
    ok = replayq:ack_sync(MQ, AckRef1),
    ok = replayq:ack_sync(DQ, AckRef2),
    {MQ, DQ};
apply_op(MQ0, DQ0, reopen, MQCfg, DQCfg) ->
    All = dump(MQ0),
    ok = replayq:close(MQ0),
    MQ1 = replayq:open(MQCfg),
    MQ = replayq:append(MQ1, All),
    ok = replayq:close(DQ0),
    DQ = replayq:open(DQCfg),
    {MQ, DQ}.

data_dir() -> filename:join(["./test-data", "prop-tests"]).

prop_seg_bytes() -> proper_types:integer(100, 1000).

prop_items() ->
    proper_types:list(proper_types:binary()).

prop_pop_args() ->
    {_Bytes = proper_types:integer(1, 1000), _Count = proper_types:integer(1, 10)}.

prop_op_list(IsOffload) ->
    Base = [{append, prop_items()}, {pop_ack, prop_pop_args()}],
    Union =
        case IsOffload of
            %% can not support reopen in proptest
            true -> Base;
            false -> [reopen | Base]
        end,
    proper_types:list(proper_types:oneof(Union)).

delete_dir(Dir) ->
    lists:foreach(
        fun(F) -> ok = file:delete(filename:join([Dir, F])) end,
        filelib:wildcard("*", Dir)
    ),
    ok = file:del_dir(Dir).

compare_stats(MQ, DQ) ->
    ?assertEqual(replayq:count(MQ), replayq:count(DQ)),
    ?assertEqual(replayq:bytes(MQ), replayq:bytes(DQ)),
    ok.

compare(Q1, Q2) ->
    {NewQ1, _, Items1} = replayq:pop(Q1, #{count_limit => 1}),
    {NewQ2, _, Items2} = replayq:pop(Q2, #{count_limit => 1}),
    case Items1 =:= Items2 of
        %% done
        true when Items1 =:= [] -> ok;
        true -> compare(NewQ1, NewQ2);
        false -> throw({diff, {Items1, NewQ1}, {Items2, NewQ2}})
    end.

%% dump all items in the queue
dump(Q) ->
    Count = replayq:count(Q),
    {_, _, All} = replayq:pop(Q, #{count_limit => Count + 1}),
    All.
