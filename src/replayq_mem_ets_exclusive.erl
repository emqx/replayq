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

%% @doc Exclusively owned ETS table for in-memory queue.
-module(replayq_mem_ets_exclusive).

-behaviour(replayq_mem).

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

-type id() :: integer().
-opaque queue() :: #{ets_tab := ets:tab(), next_id := id()}.

%% @doc Create a new queue.
-spec new(_) -> queue().
new(_) ->
    Tab = ets:new(?MODULE, [ordered_set, public]),
    #{ets_tab => Tab, next_id => 1}.

%% @doc Peek all items from the queue.
-spec peek_all(queue()) -> [term()].
peek_all(#{ets_tab := Tab}) ->
    lists:map(fun({_Id, Item}) -> Item end, ets:tab2list(Tab)).

%% @doc Peek the front item of the queue.
-spec peek(queue()) -> empty | {value, term()}.
peek(#{ets_tab := Tab}) ->
    case ets:first(Tab) of
        '$end_of_table' ->
            empty;
        Id ->
            Item = ets:lookup_element(Tab, Id, 2),
            {value, Item}
    end.

%% @doc Return 'true' if the queue is empty.
-spec is_empty(queue()) -> boolean().
is_empty(#{ets_tab := Tab}) ->
    ets:first(Tab) =:= '$end_of_table'.

%% @doc Enqueue an item to the queue.
-spec in(term(), queue()) -> queue().
in(Item, #{ets_tab := Tab, next_id := NextId} = Q) ->
    ets:insert(Tab, {NextId, Item}),
    Q#{next_id => NextId + 1}.

%% @doc Enqueue an item in reverse order to the queue.
-spec in_r(term(), queue()) -> queue().
in_r(Item, #{ets_tab := Tab} = Q) ->
    case ets:first(Tab) of
        '$end_of_table' ->
            in(Item, Q);
        Id ->
            ets:insert(Tab, {Id - 1, Item}),
            Q
    end.

%% @doc Enqueue a batch of items to the queue.
-spec in_batch([term()], queue()) -> queue().
in_batch(Items, #{ets_tab := Tab, next_id := NextId} = Q) ->
    {NewNextId, Inserts} = lists:foldl(
        fun(Item, {Id, Acc}) -> {Id + 1, [{Id, Item} | Acc]} end, {NextId, []}, Items
    ),
    ets:insert(Tab, Inserts),
    Q#{next_id => NewNextId}.

%% @doc Dequeue an item from the queue.
-spec out(queue()) -> {empty, queue()} | {{value, term()}, queue()}.
out(#{ets_tab := Tab} = Q) ->
    case ets:first(Tab) of
        '$end_of_table' ->
            {empty, Q};
        Id ->
            [{_, Item}] = ets:take(Tab, Id),
            {{value, Item}, Q}
    end.

%% @doc Destroy the queue.
-spec destroy(queue()) -> ok.
destroy(#{ets_tab := Tab}) ->
    case ets:info(Tab, owner) of
        undefined ->
            %% already deleted
            ok;
        _Owner ->
            ets:delete(Tab),
            ok
    end.
