%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Shared ETS table for in-memory queue.
%% Each item in the queue is a tuple of the form `{Id, Item}`.
%% where `Id` is a tuple of `{self(), NextId}` and `NextId` is an increasing integer.
%% Each owner process can only create one queue.
-module(replayq_mem_ets_shared).

-behaviour(replayq_mem).

-export([
    boot_init/0,
    purge_by_owner/1
]).

-export([
    new/1,
    peek_all/1,
    peek/1,
    is_empty/1,
    in/2,
    in_r/2,
    in_batch/2,
    out/1,
    destroy/1
]).

-export_type([queue/0]).

%% parse-transform for ets:fun2ms/1
-include_lib("stdlib/include/ms_transform.hrl").

-define(ETS_TAB, ?MODULE).
-define(KEY(PID, ID), {PID, ID}).

-type key() :: {pid(), non_neg_integer()}.
-type item() :: term().
-opaque queue() :: #{next_id := non_neg_integer()}.

%% @doc Create the globally shared ETS table.
-spec boot_init() -> ok.
boot_init() ->
    _ = ets:new(?ETS_TAB, [
        ordered_set,
        named_table,
        public,
        {read_concurrency, true},
        {write_concurrency, true}
    ]),
    ok.

%% @doc Create a new queue.
-spec new(_) -> queue().
new(Opts) ->
    Owner = maps:get(owner, Opts, self()),
    ok = replayq_registry:register_slot_owner(Owner),
    #{next_id => 1, owner => Owner}.

%% @doc Peek all items from the queue.
-spec peek_all(queue()) -> [item()].
peek_all(Q) ->
    Owner = owner(Q),
    Ms = ets:fun2ms(fun({?KEY(Pid, '_'), Item}) when Pid =:= Owner -> Item end),
    ets:select(?ETS_TAB, Ms).

%% @doc Peek the front item from the queue.
-spec peek(queue()) -> empty | {value, item()}.
peek(Q) ->
    Owner = owner(Q),
    case first_key(Owner) of
        {key, Key} ->
            Item = ets:lookup_element(?ETS_TAB, Key, 2),
            {value, Item};
        empty ->
            empty
    end.

%% @doc Check if a queue is empty.
-spec is_empty(queue()) -> boolean().
is_empty(Q) ->
    Owner = owner(Q),
    empty =:= first_key(Owner).

%% @doc Enqueue an item into the queue.
-spec in(item(), queue()) -> queue().
in(Item, #{next_id := NextId} = Q) ->
    Owner = owner(Q),
    Key = ?KEY(Owner, NextId),
    ets:insert(?ETS_TAB, {Key, Item}),
    Q#{next_id => NextId + 1}.

%% @doc Enqueue an item into the queue in reverse order.
-spec in_r(item(), queue()) -> queue().
in_r(Item, Q) ->
    Owner = owner(Q),
    case first_key(Owner) of
        {key, ?KEY(Owner, Id)} ->
            %% Id is the key of the first item in the queue
            %% do not allow inverse order enqueue
            Id =< 1 andalso erlang:error(badarg),
            PrevKey = ?KEY(Owner, Id - 1),
            ets:insert(?ETS_TAB, {PrevKey, Item}),
            Q;
        empty ->
            in(Item, Q)
    end.

%% @doc Enqueue a batch of items into the queue.
-spec in_batch([item()], queue()) -> queue().
in_batch(Items, #{next_id := NextId} = Q) ->
    Owner = owner(Q),
    {NewNextId, Inserts} = lists:foldl(
        fun(Item, {Id, Acc}) -> {Id + 1, [{?KEY(Owner, Id), Item} | Acc]} end,
        {NextId, []},
        Items
    ),
    ets:insert(?ETS_TAB, Inserts),
    Q#{next_id => NewNextId}.

%% @doc Dequeue an item from the queue.
-spec out(queue()) -> {empty, queue()} | {{value, item()}, queue()}.
out(Q) ->
    Owner = owner(Q),
    case first_key(Owner) of
        {key, ?KEY(Owner, Id)} ->
            [{_, Item}] = ets:take(?ETS_TAB, ?KEY(Owner, Id)),
            {{value, Item}, Q};
        empty ->
            {empty, Q}
    end.

%% @doc Destroy the queue.
-spec destroy(queue()) -> ok.
destroy(Q) ->
    Owner = owner(Q),
    ok = purge_by_owner(Owner),
    ok = replayq_registry:deregister_slot_owner(Owner).

%% @doc Purge the queue by owner pid.
-spec purge_by_owner(pid()) -> ok.
purge_by_owner(Owner) ->
    Ms = ets:fun2ms(fun({?KEY(Pid, '_'), _}) -> Pid =:= Owner end),
    ets:select_delete(?ETS_TAB, Ms),
    ok.

%% Get the first key in the queue.
-spec first_key(pid()) -> empty | {key, key()}.
first_key(Owner) ->
    case ets:next(?ETS_TAB, ?KEY(Owner, 0)) of
        ?KEY(Pid, _) = Key when Pid =:= Owner ->
            {key, Key};
        _ ->
            empty
    end.

owner(#{owner := Owner}) -> Owner.
