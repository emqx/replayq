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

-module(replayq_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").

all() ->
    [
        {group, queue},
        {group, ets_exclusive},
        {group, ets_shared}
    ].

all_cases() ->
    [
        F
     || {F, _} <- ?MODULE:module_info(exports),
        is_t_function(atom_to_list(F))
    ].

groups() ->
    [
        {queue, [], all_cases()},
        {ets_exclusive, [], all_cases()},
        {ets_shared, [], all_cases() ++ [owner_down_cause_purge]}
    ].

init_per_group(Group, Config) ->
    [{ct_group, Group} | Config].

end_per_group(_Group, _Config) ->
    ok.

is_t_function("t_" ++ _) -> true;
is_t_function(_) -> false.

-define(SUFFIX, "replaylog").
-define(DIR, filename:join([data_dir(), ?FUNCTION_NAME, integer_to_list(uniq())])).

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(replayq),
    Config.

end_per_suite(_Config) ->
    ok.

%% the very first run
t_init(CtConfig) ->
    Dir = ?DIR,
    Config = #{dir => Dir, seg_bytes => 100},
    Q1 = open(CtConfig, Config),
    ?assertEqual(0, replayq:count(Q1)),
    ?assertEqual(0, replayq:bytes(Q1)),
    ok = replayq:close(Q1),
    Q2 = open(CtConfig, Config),
    ?assertEqual(0, replayq:count(Q2)),
    ?assertEqual(0, replayq:bytes(Q2)),
    ok = replayq:close_and_purge(Q2).

t_reopen(CtConfig) ->
    Dir = ?DIR,
    Config = #{dir => Dir, seg_bytes => 100},
    Q0 = open(CtConfig, Config),
    Q1 = replayq:append(Q0, [<<"item1">>, <<"item2">>]),
    ok = replayq:close(Q1),
    Q2 = open(CtConfig, Config),
    ?assertEqual(2, replayq:count(Q2)),
    ?assertEqual(10, replayq:bytes(Q2)),
    ok = replayq:close(Q2),
    %% ok to close after closed already
    ok = replayq:close_and_purge(Q2).

t_volatile(CtConfig) ->
    Dir = ?DIR,
    Config = #{dir => Dir, seg_bytes => 100},
    Q0 = open(CtConfig, Config),
    Q1 = replayq:append(Q0, [<<"item1">>, <<"item2">>]),
    ok = replayq:close(Q1),
    Q2 = open(CtConfig, Config#{offload => {true, volatile}}),
    ?assertEqual(0, replayq:count(Q2)),
    ?assertEqual(0, replayq:bytes(Q2)),
    {Q3, _QAckRef, Items} = replayq:pop(Q2, #{count_limit => 10}),
    ?assertEqual([], Items),
    ok = replayq:close_and_purge(Q3).

%% when popping from in-mem segment, the segment size stats may overflow
%% but not consuming as much memory
t_offload_in_mem_seg_overflow(CtConfig) ->
    Dir = ?DIR,
    Config = #{dir => Dir, seg_bytes => 11, offload => true},
    Q0 = open(CtConfig, Config),
    % in mem, one byte left
    Q1 = replayq:append(Q0, [<<"item1">>, <<"item2">>]),
    % not offloading to disk yet
    ?assertMatch([], list_segments(Dir)),
    ?assertMatch(#{w_cur := #{fd := no_fd}}, Q1),
    {Q2, _AckRef1, Items1} = replayq:pop(Q1, #{}),
    ?assertEqual([<<"item1">>, <<"item2">>], Items1),
    %% still in mem
    Q3 = replayq:append(Q2, [<<"item3">>]),
    % not offloading to disk yet
    ?assertMatch([], list_segments(Dir)),
    ?assertMatch(#{w_cur := #{fd := no_fd}}, Q3),
    ok = replayq:close(Q3),
    ok = replayq:close_and_purge(Q3).

t_offload_file(CtConfig) ->
    Dir = ?DIR,
    Config = #{dir => Dir, seg_bytes => 10, offload => true},
    Q0 = open(CtConfig, Config),
    % in mem
    Q1 = replayq:append(Q0, [<<"item1">>]),
    ?assertMatch([], list_segments(Dir)),
    ?assertMatch(#{w_cur := #{fd := no_fd}}, Q1),
    % in mem, but trigger seg 2 to create
    Q2 = replayq:append(Q1, [<<"item2">>]),
    ?assertEqual([filename(2)], list_segments(Dir)),
    % in seg 2, trigger seg 3 to create
    Q3 = replayq:append(Q2, [<<"item3">>, <<"item4">>]),
    ?assertEqual([filename(2), filename(3)], list_segments(Dir)),
    {Q4, AckRef1, Items1} = replayq:pop(Q3, #{count_limit => 2}),
    ?assertEqual([<<"item1">>, <<"item2">>], Items1),
    ok = replayq:ack_sync(Q4, AckRef1),
    ?assertEqual([filename(2), filename(3)], list_segments(Dir)),
    {Q5, AckRef2, Items2} = replayq:pop(Q4, #{}),
    ?assertEqual({2, 2}, AckRef2),
    ?assertEqual([<<"item3">>, <<"item4">>], Items2),
    %% caught up
    ok = replayq:ack_sync(Q5, AckRef2),
    ?assertEqual([filename(2), filename(3)], list_segments(Dir)),
    % in seg 3
    Q6 = replayq:append(Q5, [<<"item5">>]),
    {Q7, AckRef3, Items3} = replayq:pop(Q6, #{}),
    ?assertEqual([<<"item5">>], Items3),
    ok = replayq:ack_sync(Q7, AckRef3),
    ok = replayq:close(Q7),
    ok = replayq:close_and_purge(Q7).

t_offload_reopen(CtConfig) ->
    Dir = ?DIR,
    Config = #{dir => Dir, seg_bytes => 100, offload => true},
    Q0 = open(CtConfig, Config),
    Q1 = replayq:append(Q0, [<<"item1">>, <<"item2">>]),
    ?assertMatch(#{w_cur := #{fd := no_fd}}, Q1),
    % should be filtered out
    put(noise, noise),
    {Q2, _AckRef, Items} = replayq:pop(Q1, #{count_limit => 1}),
    ?assertEqual([<<"item1">>], Items),
    ok = replayq:close(Q2),
    Q3 = open(CtConfig, Config),
    ?assertEqual(2, replayq:count(Q3)),
    ?assertEqual(10, replayq:bytes(Q3)),
    {Q4, AckRef1, Items1} = replayq:pop(Q3, #{count_limit => 2}),
    ?assertEqual([<<"item1">>, <<"item2">>], Items1),
    ?assertEqual([filename(1), filename(2)], list_segments(Dir)),
    ok = replayq:ack_sync(Q4, AckRef1),
    ok = replayq:close(Q4),
    Q5 = open(CtConfig, Config),
    ?assertEqual(0, replayq:count(Q5)),
    ?assertEqual(0, replayq:bytes(Q5)),
    ok = replayq:close_and_purge(Q5).

t_reopen_v0(CtConfig) ->
    Dir = ?DIR,
    Config = #{dir => Dir, seg_bytes => 1000},
    Q0 = open(CtConfig, Config),
    % inspect the opaque internal structure for test
    #{w_cur := #{fd := Fd}} = Q0,
    % append a version-0 item
    file:write(Fd, make_v0_iodata(<<"item1">>)),
    % append a version-1 itme
    Q2 = replayq:append(Q0, [<<"item2">>]),
    ok = replayq:close(Q2),
    Q3 = open(CtConfig, Config),
    {Q4, _AckRef, Items} = replayq:pop(Q3, #{count_limit => 3}),
    %% do not expect item3 because it was appended to a corrupted tail
    ?assertEqual([<<"item1">>, <<"item2">>], Items),
    ?assert(replayq:is_empty(Q4)),
    ok = replayq:close_and_purge(Q4).

t_append_pop_disk_default_marshaller(CtConfig) ->
    Dir = ?DIR,
    Config = #{dir => Dir, seg_bytes => 1},
    test_append_pop_disk(CtConfig, Config).

t_append_pop_disk_my_marshaller(CtConfig) ->
    Dir = ?DIR,
    Config = #{
        dir => Dir,
        seg_bytes => 1,
        sizer => fun(Item) -> size(Item) end,
        marshaller => fun
            (<<"mmp", I/binary>>) -> I;
            (I) -> <<"mmp", I/binary>>
        end
    },
    test_append_pop_disk(CtConfig, Config).

test_append_pop_disk(CtConfig, Config) ->
    Q0 = open(CtConfig, Config),
    Q1 = replayq:append(Q0, [<<"item1">>, <<"item2">>]),
    Q2 = replayq:append(Q1, [<<"item3">>]),
    ?assertEqual(<<"item1">>, replayq:peek(Q2)),
    {Q3, AckRef, Items} = replayq:pop(Q2, #{
        count_limit => 5,
        bytes_limit => 1000
    }),
    ?assertEqual([<<"item1">>, <<"item2">>, <<"item3">>], Items),
    %% stop without acking
    ok = replayq:close(Q3),
    %% open again expect to receive the same items
    Q4 = open(CtConfig, Config),
    {Q5, AckRef1, Items1} = replayq:pop(Q4, #{
        count_limit => 5,
        bytes_limit => 1000
    }),
    ?assertEqual(AckRef, AckRef1),
    ?assertEqual(Items, Items1),
    lists:foreach(fun(_) -> ok = replayq:ack(Q5, AckRef) end, lists:seq(1, 100)),
    ok = replayq:close(Q5),
    Q6 = open(CtConfig, Config),
    ?assert(replayq:is_empty(Q6)),
    ?assertEqual(empty, replayq:peek(Q6)),
    ?assertEqual({Q6, nothing_to_ack, []}, replayq:pop(Q6, #{})),
    ok = replayq:ack(Q6, nothing_to_ack),
    ok = replayq:close_and_purge(Q6).

t_append_pop_mem_default_marshaller(CtConfig) ->
    Config = #{mem_only => true},
    test_append_pop_mem(CtConfig, Config).

t_append_pop_mem_my_marshaller(CtConfig) ->
    Config = #{
        mem_only => true,
        sizer => fun(Item) -> size(Item) end,
        marshaller => fun
            (<<"mmp", I/binary>>) -> I;
            (I) -> <<"mmp", I/binary>>
        end
    },
    test_append_pop_mem(CtConfig, Config).

test_append_pop_mem(CtConfig, Config) ->
    Q0 = open(CtConfig, Config),
    Q1 = replayq:append(Q0, [<<"item1">>, <<"item2">>]),
    Q2 = replayq:append(Q1, [<<"item3">>]),
    ?assertEqual(<<"item1">>, replayq:peek(Q2)),
    {Q3, _AckRef, Items} = replayq:pop(Q2, #{
        count_limit => 5,
        bytes_limit => 1000
    }),
    ?assertEqual([<<"item1">>, <<"item2">>, <<"item3">>], Items),
    %% stop without acking
    ok = replayq:close(Q3),
    %% open again expect to receive the same items
    Q4 = open(CtConfig, Config),
    {Q5, _AckRef1, _Items1} =
        replayq:pop(Q4, #{count_limit => 5, bytes_limit => 1000}),
    ?assertEqual(empty, replayq:peek(Q5)),
    ?assertEqual({Q5, nothing_to_ack, []}, replayq:pop(Q5, #{})),
    ok = replayq:ack(Q5, nothing_to_ack),
    ok = replayq:close(Q5).

t_append_max_total_bytes_mem(CtConfig) ->
    Config = #{
        mem_only => true,
        sizer => fun(Item) -> size(Item) end,
        marshaller => fun
            (<<"mmp", I/binary>>) -> I;
            (I) -> <<"mmp", I/binary>>
        end,
        max_total_bytes => 10
    },
    test_append_max_total_bytes(CtConfig, Config).

t_append_max_total_bytes_disk(CtConfig) ->
    Dir = ?DIR,
    Config = #{
        dir => Dir,
        seg_bytes => 1,
        sizer => fun(Item) -> size(Item) end,
        marshaller => fun
            (<<"mmp", I/binary>>) -> I;
            (I) -> <<"mmp", I/binary>>
        end,
        max_total_bytes => 10
    },
    test_append_max_total_bytes(CtConfig, Config).

test_append_max_total_bytes(CtConfig, Config) ->
    Q0 = open(CtConfig, Config),
    ?assertEqual(-10, replayq:overflow(Q0)),
    Q1 = replayq:append(Q0, [<<"item1">>, <<"item2">>, <<"item3">>, <<"item4">>]),
    ?assertEqual(10, replayq:overflow(Q1)),
    {Q2, _AckRef, _Items} = replayq:pop(Q1, #{count_limit => 2}),
    ?assertEqual(0, replayq:overflow(Q2)),
    ok = replayq:close_and_purge(Q2).

t_pop_limit_disk(CtConfig) ->
    Dir = ?DIR,
    Config = #{dir => Dir, seg_bytes => 1},
    ok = test_pop_limit(CtConfig, Config).

t_pop_limit_mem(CtConfig) ->
    Config = #{mem_only => true},
    ok = test_pop_limit(CtConfig, Config).

test_pop_limit(CtConfig, Config) ->
    Q0 = open(CtConfig, Config),
    Q1 = replayq:append(Q0, [<<"item1">>, <<"item2">>]),
    Q2 = replayq:append(Q1, [<<"item3">>]),
    {Q3, _AckRef1, Items1} = replayq:pop(Q2, #{
        count_limit => 1,
        bytes_limit => 1000
    }),
    ?assertEqual([<<"item1">>], Items1),
    {Q4, _AckRef2, Items2} = replayq:pop(Q3, #{
        count_limit => 10,
        bytes_limit => 1
    }),
    ?assertEqual([<<"item2">>], Items2),
    ok = replayq:close_and_purge(Q4).

t_commit_in_the_middle(CtConfig) ->
    Dir = ?DIR,
    Config = #{dir => Dir, seg_bytes => 1000},
    Q0 = open(CtConfig, Config),
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
    Q4 = open(CtConfig, Config),
    {Q5, _AckRef2, Items2} = replayq:pop(Q4, #{count_limit => 1}),
    ?assertEqual([<<"item2">>], Items2),
    ?assertEqual(1, replayq:count(Q5)),
    ?assertEqual(5, replayq:bytes(Q5)),
    ok = replayq:close_and_purge(Q5).

t_first_segment_corrupted(CtConfig) ->
    Dir = ?DIR,
    SegBytes = 10,
    Config = #{dir => Dir, seg_bytes => SegBytes},
    Item = iolist_to_binary(lists:duplicate(SegBytes, "a")),
    Q0 = open(CtConfig, Config),
    ok = corrupt(Q0),
    %% to the first (corrputed) segment
    Q1 = replayq:append(Q0, [Item]),
    %% to the second segment
    Q2 = replayq:append(Q1, [Item]),
    %% assert it has rolled to the 3rd segment
    ?assertMatch(#{head_segno := 1, w_cur := #{segno := 3}}, Q2),
    replayq:close(Q2),
    %% reopen to discover that the first segment is corrupted
    Q3 = open(CtConfig, Config),
    ?assertEqual(1, replayq:count(Q3)),
    {Q4, _AckRef, Items} = replayq:pop(Q3, #{count_limit => 3}),
    ?assertEqual([Item], Items),
    ?assert(replayq:is_empty(Q4)),
    ok = replayq:close_and_purge(Q4).

t_second_segment_corrupted(CtConfig) ->
    Dir = ?DIR,
    SegBytes = 10,
    Config = #{dir => Dir, seg_bytes => SegBytes},
    Item = iolist_to_binary(lists:duplicate(SegBytes, "a")),
    Q0 = open(CtConfig, Config),
    %% to the first segment
    Q1 = replayq:append(Q0, [Item]),
    ok = corrupt(Q1),
    %% to the second (corrupted) segment
    Q2 = replayq:append(Q1, [Item]),
    %% to the thrid segment
    Q3 = replayq:append(Q2, [Item]),
    %% assert it has rolled to the 4th segment
    ?assertMatch(#{head_segno := 1, w_cur := #{segno := 4}}, Q3),
    replayq:close(Q3),
    %% reopen to discover that the second segment is corrupted
    Q4 = open(CtConfig, Config),
    ?assertEqual(2, replayq:count(Q4)),
    {Q5, _AckRef, Items} = replayq:pop(Q4, #{count_limit => 3}),
    ?assertEqual([Item, Item], Items),
    ?assert(replayq:is_empty(Q5)),
    ok = replayq:close_and_purge(Q5).

t_last_segment_corrupted(CtConfig) ->
    Dir = ?DIR,
    SegBytes = 10,
    Config = #{dir => Dir, seg_bytes => SegBytes},
    Item = iolist_to_binary(lists:duplicate(SegBytes, "a")),
    Q0 = open(CtConfig, Config),
    %% to the first segment
    Q1 = replayq:append(Q0, [Item]),
    %% to the second segment
    Q2 = replayq:append(Q1, [Item]),
    ok = corrupt(Q2),
    %% to the thrid (corrupted) segment
    Q3 = replayq:append(Q2, [<<"thridsegment">>]),
    replayq:close(Q3),
    %% reopen to discover that the third segment is corrupted
    Q4 = open(CtConfig, Config),
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
    Q8 = open(CtConfig, Config),
    ?assert(replayq:is_empty(Q8)),
    replayq:ack(Q7, AckRef),
    replayq:close_and_purge(Q7).

t_corrupted_segment(CtConfig) ->
    ?assert(test_corrupted_segment(CtConfig, <<"foo">>)),
    ?assert(test_corrupted_segment(CtConfig, <<0:8, 0:32, 1:32, 1:8>>)),
    ?assert(test_corrupted_segment(CtConfig, <<0:8, 0:32, 0:32, "randomtail">>)),
    ?assert(test_corrupted_segment(CtConfig, <<1:8, 0:32, 1:32, 1:8>>)),
    ?assert(test_corrupted_segment(CtConfig, <<1:8, 841265288:32, 0:32, 1:32, 1:8>>)).

test_corrupted_segment(CtConfig, BadBytes) ->
    Dir = ?DIR,
    Config = #{dir => Dir, seg_bytes => 1000},
    Q0 = open(CtConfig, Config),
    Item2 = <<>>,
    Q1 = replayq:append(Q0, [<<"item1">>, Item2]),
    % inspect the opaque internal structure for test
    #{w_cur := #{fd := Fd}} = Q1,
    % corrupt the file
    file:write(Fd, BadBytes),
    Q2 = replayq:append(Q0, [<<"item3">>]),
    ok = replayq:close(Q2),
    Q3 = open(CtConfig, Config),
    {Q4, _AckRef, Items} = replayq:pop(Q3, #{count_limit => 3}),
    %% do not expect item3 because it was appended to a corrupted tail
    ?assertEqual([<<"item1">>, Item2], Items),
    ?assert(replayq:is_empty(Q4)),
    ok = replayq:close_and_purge(Q4),
    true.

t_comitter_crash(CtConfig) ->
    Dir = ?DIR,
    Config = #{dir => Dir, seg_bytes => 1000},
    #{committer := Committer} = open(CtConfig, Config),
    erlang:process_flag(trap_exit, true),
    Committer ! <<"foo">>,
    receive
        {'EXIT', _Pid, {replayq_committer_unkown_msg, <<"foo">>}} ->
            ok
    end.

%% Checks that our spawned committer can register a name for itself when using filepaths
%% larger than 255 bytes.
t_huge_filepath(CtConfig) ->
    Dir0 = ?DIR,
    Dir = filename:join(Dir0, binary:copy(<<"a">>, 255)),
    Config = #{dir => Dir, seg_bytes => 1000},
    Q = #{committer := Committer} = open(CtConfig, Config),
    ?assert(is_process_alive(Committer)),
    replayq:close(Q),
    ok.

%% Checks that we don't allow having the same directory open by multiple replayqs.
t_same_directory_committer_clash(CtConfig) ->
    Dir = ?DIR,
    Config = #{dir => Dir, seg_bytes => 1000},
    Q1 = open(CtConfig, Config),
    try open(CtConfig, Config) of
        Q2 -> error({"should not allow opening a second replayq", Q2})
    catch
        error:{badmatch, {error, {already_registered, Dir1}}} ->
            ?assertEqual(iolist_to_binary(Dir), Dir1),
            ok
    end,
    replayq:close(Q1),
    ok.

t_is_mem_only_mem(CtConfig) ->
    Q = open(CtConfig, #{mem_only => true}),
    true = replayq:is_mem_only(Q),
    ok = replayq:close(Q).

t_is_mem_only_disk(CtConfig) ->
    Config = #{dir => ?DIR, seg_bytes => 100},
    Q = open(CtConfig, Config),
    false = replayq:is_mem_only(Q),
    ok = replayq:close(Q).

t_is_writing_to_disk_mem_only(CtConfig) ->
    Q0 = open(CtConfig, #{mem_only => true}),
    false = replayq:is_writing_to_disk(Q0),
    Q1 = replayq:append(Q0, [<<"item1">>, <<"item2">>]),
    false = replayq:is_writing_to_disk(Q1),
    Q2 = replayq:append(Q1, [<<"item3">>]),
    false = replayq:is_writing_to_disk(Q2),
    ok = replayq:close(Q2).

t_is_writing_to_disk_disk(CtConfig) ->
    Dir = ?DIR,
    Config = #{dir => Dir, seg_bytes => 100},
    Q0 = open(CtConfig, Config),
    true = replayq:is_writing_to_disk(Q0),
    Q1 = replayq:append(Q0, [<<"item1">>, <<"item2">>]),
    true = replayq:is_writing_to_disk(Q1),
    Q2 = replayq:append(Q1, [<<"item3">>]),
    true = replayq:is_writing_to_disk(Q2),
    ok = replayq:close(Q2).

t_is_writing_to_disk_offload(CtConfig) ->
    Dir = ?DIR,
    Config = #{dir => Dir, seg_bytes => 10, offload => true},
    Q0 = open(CtConfig, Config),
    % Initially in memory, should return false
    false = replayq:is_writing_to_disk(Q0),
    Q1 = replayq:append(Q0, [<<"item1">>]),
    % Still in memory
    false = replayq:is_writing_to_disk(Q1),
    % Trigger creation of seg 2 by appending more items
    Q2 = replayq:append(Q1, [<<"item2">>]),
    % Now writing to disk segment, should return true
    true = replayq:is_writing_to_disk(Q2),
    Q3 = replayq:append(Q2, [<<"item3">>, <<"item4">>]),
    % Still writing to disk
    true = replayq:is_writing_to_disk(Q3),
    ok = replayq:close_and_purge(Q3).

t_stop_before_mem(CtConfig) ->
    Config = #{mem_only => true},
    stop_before_test(CtConfig, Config),
    stop_before_readme_example_test(CtConfig, Config).

t_stop_before_disk(CtConfig) ->
    Config1 = #{
        dir => ?DIR,
        seg_bytes => 100
    },
    stop_before_test(CtConfig, Config1),
    Config2 = #{
        dir => filename:join([?DIR, "example"]),
        seg_bytes => 100
    },
    stop_before_readme_example_test(CtConfig, Config2).

stop_before_test(CtConfig, Config) ->
    Q0 = open(CtConfig, Config),
    Q1 = replayq:append(Q0, [<<"1">>, <<"2">>, <<"3">>, <<"4">>, <<"5">>]),
    StopBeforeFun =
        fun
            (_Item, #{stop_ctr := 3}) ->
                true;
            (_Item, #{stop_ctr := Cnt}) ->
                #{stop_ctr => Cnt + 1}
        end,
    {Q2, AckRef, Items} =
        replayq:pop(
            Q1,
            #{
                count_limit => 100,
                bytes_limit => 10000000,
                stop_before => {StopBeforeFun, #{stop_ctr => 0}}
            }
        ),
    ok = replayq:ack(Q2, AckRef),
    ?assertEqual([<<"1">>, <<"2">>, <<"3">>], Items),
    ?assertEqual(2, replayq:count(Q2)),
    ok = replayq:close(Q2).

%% Test that the example in the readme file works
stop_before_readme_example_test(CtConfig, Config) ->
    Q0 = open(CtConfig, Config),
    Q1 = replayq:append(
        Q0,
        [
            <<"type1">>,
            <<"type1">>,
            <<"type2">>,
            <<"type2">>,
            <<"type2">>,
            <<"type3">>
        ]
    ),
    StopBeforeFunc =
        fun
            (Item, #{current_type := none}) ->
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

t_corrupted_commit(CtConfig) ->
    Dir = ?DIR,
    Config = #{dir => Dir, seg_bytes => 1000},
    Q0 = open(CtConfig, Config),
    Q1 = replayq:append(Q0, [<<"item1">>]),
    {Q2, AckRef, _} = replayq:pop(Q1, #{count_limit => 3}),
    ok = replayq:ack_sync(Q2, AckRef),
    ok = replayq:close(Q2),
    CommitFile = filename:join(Dir, "COMMIT"),
    ?assertMatch({ok, [_]}, file:consult(CommitFile)),

    ok = file:write_file(CommitFile, <<>>),
    %% assert no crash
    Q3 = open(CtConfig, Config),
    ok = replayq:close(Q3),

    ok = file:write_file(CommitFile, <<"bad-erlang-term">>),
    %% assert no crash
    Q4 = open(CtConfig, Config),
    ok = replayq:close_and_purge(Q4).

t_pop_bytes_mem(CtConfig) ->
    Config = #{
        mem_only => true,
        seg_bytes => 1000,
        sizer => fun(Item) -> size(Item) end
    },
    ok = test_pop_bytes(CtConfig, Config, default),
    ok = test_pop_bytes(CtConfig, Config, at_most),
    ok = test_pop_bytes(CtConfig, Config, at_least),
    ok.

t_pop_bytes_disk(CtConfig) ->
    Dir = ?DIR,
    Config = #{
        dir => Dir,
        seg_bytes => 1000,
        sizer => fun(Item) -> size(Item) end
    },
    ok = test_pop_bytes(CtConfig, Config, default),
    ok = test_pop_bytes(CtConfig, Config, at_most),
    ok = test_pop_bytes(CtConfig, Config, at_least).

test_pop_bytes(CtConfig, Config, BytesMode) ->
    Q0 = open(CtConfig, Config),
    %% Two 5 bytes elements
    Item1 = <<"12345">>,
    Item2 = <<"67890">>,
    Q1 = replayq:append(Q0, [Item1, Item2]),
    ItemSize = 5,
    ?assertEqual(ItemSize * 2, replayq:bytes(Q1)),
    case BytesMode of
        default ->
            %% Default behavior: we pop _at most_ N bytes, and return at least 1 item, if any.
            %% Asking for less bytes than the 2 elements should yield singleton batch.
            ?assertEqual([Item1], spop(Q1, #{count_limit => 10, bytes_limit => ItemSize - 1}));
        at_most ->
            ?assertEqual(
                [Item1], spop(Q1, #{count_limit => 10, bytes_limit => {at_most, ItemSize - 1}})
            );
        at_least ->
            %% ... but dropping _at least_ less bytes than 1 item should drop both of them.
            ?assertEqual(
                [Item1, Item2],
                spop(Q1, #{count_limit => 10, bytes_limit => {at_least, ItemSize + 1}})
            )
    end,
    ok = replayq:close_and_purge(Q0).

owner_down_cause_purge(CtConfig) ->
    {Owner, Ref} = spawn_monitor(fun() ->
        receive
            stop -> ok
        end
    end),
    Q = open(CtConfig, #{
        mem_only => true,
        mem_queue_module => replayq_mem_ets_shared,
        mem_queue_opts => #{owner => Owner}
    }),
    Q1 = replayq:append(Q, [<<"item1">>]),
    ?assertEqual(1, replayq:count(Q1)),
    ?assertEqual(<<"item1">>, replayq:peek(Q1)),
    Owner ! stop,
    receive
        {'DOWN', Ref, process, Owner, _Reason} ->
            ok
    end,
    _ = sys:get_state(replayq_registry),
    %% the counter is still kept in the Q1 term
    ?assertEqual(1, replayq:count(Q1)),
    %% but the queue is empty
    ?assertEqual(empty, replayq:peek(Q1)),
    ok.

%% helpers ===========================================================

%% simple-pop: pop the queue and return the items
spop(Q, Opts) ->
    {_Q1, _AckRef, Items} = replayq:pop(Q, Opts),
    Items.

list_segments(Dir) -> filelib:wildcard("*." ?SUFFIX, Dir).

data_dir() -> "./test-data".

uniq() ->
    {_, _, Micro} = erlang:timestamp(),
    Micro.

make_v0_iodata(Item) ->
    Size = size(Item),
    CRC = erlang:crc32(Item),
    [<<0:8, CRC:32/unsigned-integer, Size:32/unsigned-integer>>, Item].

filename(Segno) ->
    lists:flatten(io_lib:format("~10.10.0w." ?SUFFIX, [Segno])).

%% corrupt the segment
corrupt(#{w_cur := #{fd := Fd}}) ->
    file:write(Fd, "some random bytes").

open(CtConfig, Config0) ->
    Config =
        case proplists:get_value(ct_group, CtConfig) of
            queue -> Config0;
            ets_exclusive -> Config0#{mem_queue_module => replayq_mem_ets_exclusive};
            ets_shared -> Config0#{mem_queue_module => replayq_mem_ets_shared}
        end,
    replayq:open(Config).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
