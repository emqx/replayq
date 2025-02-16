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
-module(replayq_mem_queue).
-behaviour(replayq_mem).

-export([
    new/1,
    to_list/1,
    peek/1,
    is_empty/1,
    in/2,
    in_r/2,
    in_batch/2,
    out/1
]).

-export_type([queue/1]).

-opaque queue(Term) :: queue:queue(Term).

%% @doc Create a new queue.
-spec new(_) -> queue(term()).
new(_) -> queue:new().

%% @doc Convert a queue to a list.
-spec to_list(queue(Term)) -> [Term].
to_list(Q) -> queue:to_list(Q).

%% @doc Peek the front item of the queue.
-spec peek(queue(Term)) -> empty | {value, Term}.
peek(Q) -> queue:peek(Q).

%% @doc Return 'true' if the queue is empty.
-spec is_empty(queue(_)) -> boolean().
is_empty(Q) -> queue:is_empty(Q).

%% @doc Enqueue an item to the queue.
-spec in(Term, queue(Term)) -> queue(Term).
in(Item, Q) -> queue:in(Item, Q).

%% @doc Enqueue an item in reverse order.
-spec in_r(Term, queue(Term)) -> queue(Term).
in_r(Item, Q) -> queue:in_r(Item, Q).

%% @doc Enqueue a batch of items to the queue.
-spec in_batch([Term], queue(Term)) -> queue(Term).
in_batch(Items, Q) ->
    case is_empty(Q) of
        true ->
            queue:from_list(Items);
        false ->
            lists:foldl(fun(Item, Q0) -> in(Item, Q0) end, Q, Items)
    end.

%% @doc Dequeue an item from the queue.
-spec out(queue(Term)) -> {empty, queue(Term)} | {{value, Term}, queue(Term)}.
out(Q) -> queue:out(Q).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
