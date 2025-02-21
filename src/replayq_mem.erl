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

%% @doc Memory-based queue for the replayq.
-module(replayq_mem).

-export([
    new/2,
    peek_all/2,
    peek/2,
    is_empty/2,
    in/3,
    in_r/3,
    in_batch/3,
    out/2,
    destroy/2
]).

-export_type([options/0, queue/1]).

-type options() :: term().
%% depends on the implementation of the queue module
-type queue(_Term) :: term().

-callback new(options()) -> queue(_).
-callback peek_all(queue(_)) -> [term()].
-callback peek(queue(_)) -> empty | {value, term()}.
-callback is_empty(queue(_)) -> boolean().
-callback in(term(), queue(_)) -> queue(_).
-callback in_batch([term()], queue(_)) -> queue(_).
-callback out(queue(_)) -> {empty, queue(_)} | {{value, term()}, queue(_)}.
-callback destroy(queue(_)) -> ok.

%% @doc Create a new queue.
-spec new(module(), options()) -> queue(_).
new(Module, Options) ->
    Module:new(Options).

%% @doc Peek all items from the queue.
-spec peek_all(module(), queue(_)) -> [term()].
peek_all(Module, Q) ->
    Module:peek_all(Q).

%% @doc Peek at the next item in the queue.
-spec peek(module(), queue(_)) -> empty | {value, term()}.
peek(Module, Q) ->
    Module:peek(Q).

%% @doc Check if the queue is empty.
-spec is_empty(module(), queue(_)) -> boolean().
is_empty(Module, Q) ->
    Module:is_empty(Q).

%% @doc Insert an item into the queue.
-spec in(module(), term(), queue(_)) -> queue(_).
in(Module, Item, Q) ->
    Module:in(Item, Q).

%% @doc Insert an item into the queue in reverse order.
%% This is only intended for returning items to the queue after out/2.
%% But not for normal inverse order enqueue.
-spec in_r(module(), term(), queue(_)) -> queue(_).
in_r(Module, Item, Q) ->
    Module:in_r(Item, Q).

%% @doc Insert a batch of items into the queue.
-spec in_batch(module(), [term()], queue(_)) -> queue(_).
in_batch(Module, Items, Q) ->
    Module:in_batch(Items, Q).

%% @doc Remove an item from the queue.
-spec out(module(), queue(_)) -> {empty, queue(_)} | {{value, term()}, queue(_)}.
out(Module, Q) ->
    Module:out(Q).

%% @doc Destroy the queue.
-spec destroy(module(), queue(_)) -> ok.
destroy(Module, Q) ->
    Module:destroy(Q).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
