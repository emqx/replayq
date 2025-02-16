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
    new/0,
    from_list/1,
    to_list/1,
    peek/1,
    is_empty/1,
    in/2,
    out/1
    ]).

-export_type([queue/1]).

-opaque queue(Term) :: queue:queue(Term).

%% @doc Create a new queue.
-spec new() -> queue(term()).
new() -> queue:new().

%% @doc Create a new queue from a list.
-spec from_list([Term]) -> queue(Term).
from_list(List) -> queue:from_list(List).

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

%% @doc Dequeue an item from the queue.
-spec out(queue(Term)) -> {empty, queue(Term)} | {value, Term, queue(Term)}.
out(Q) -> queue:out(Q).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
