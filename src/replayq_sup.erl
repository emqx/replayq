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
-module(replayq_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% `supervisor' API
-export([init/1]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%------------------------------------------------------------------------------
%% `supervisor' API
%%------------------------------------------------------------------------------

init([]) ->
    Registry = worker_spec(replayq_registry),
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    ChildSpecs = [Registry],
    {ok, {SupFlags, ChildSpecs}}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

worker_spec(Mod) ->
    #{
        id => Mod,
        start => {Mod, start_link, []},
        restart => permanent,
        shutdown => 5_000,
        type => worker
    }.
