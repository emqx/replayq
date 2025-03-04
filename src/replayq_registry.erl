%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(replayq_registry).

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    register_committer/2,
    deregister_committer/1,
    register_slot_owner/1,
    deregister_slot_owner/1
]).

%% `gen_server' API
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%% call/cast/info events
-record(register_committer, {dir :: string() | binary(), pid :: pid()}).
-record(deregister_committer, {pid :: pid()}).
-record(register_slot_owner, {pid :: pid()}).
-record(deregister_slot_owner, {pid :: pid()}).
%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec register_committer(string() | binary(), pid()) -> ok | {error, already_registered}.
register_committer(Dir0, Pid) ->
    Dir = iolist_to_binary(Dir0),
    gen_server:call(?MODULE, #register_committer{dir = Dir, pid = Pid}, infinity).

deregister_committer(Pid) ->
    gen_server:call(?MODULE, #deregister_committer{pid = Pid}, infinity).

%% @doc Register a slot owner in the global shared queue.
-spec register_slot_owner(pid()) -> ok | {error, already_registered}.
register_slot_owner(Pid) ->
    gen_server:call(?MODULE, #register_slot_owner{pid = Pid}, infinity).

-spec deregister_slot_owner(pid()) -> ok.
deregister_slot_owner(Pid) ->
    gen_server:call(?MODULE, #deregister_slot_owner{pid = Pid}, infinity).

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(_Opts) ->
    process_flag(trap_exit, true),
    State = #{committers => #{}, slot_owners => #{}},
    {ok, State}.

handle_call(#register_committer{dir = Dir, pid = Pid}, _From, State0) ->
    %% Should we expand the directory path to avoid tricks with links and relative paths?
    #{committers := Committers0} = State0,
    case Committers0 of
        #{Dir := SomePid} ->
            case is_process_alive(SomePid) of
                true ->
                    {reply, {error, {already_registered, Dir}}, State0};
                false ->
                    {_, State1} = pop_committer(State0, SomePid),
                    State = do_register_committer(State1, Dir, Pid),
                    {reply, ok, State}
            end;
        _ ->
            State = do_register_committer(State0, Dir, Pid),
            {reply, ok, State}
    end;
handle_call(#deregister_committer{pid = Pid}, _From, #{committers := Committers0} = State0) ->
    case is_map_key(Pid, Committers0) of
        false ->
            {reply, ok, State0};
        true ->
            {_, State} = pop_committer(State0, Pid),
            {reply, ok, State}
    end;
handle_call(#register_slot_owner{pid = Pid}, _From, #{slot_owners := Owners0} = State0) ->
    case is_map_key(Pid, Owners0) of
        false ->
            Ref = erlang:monitor(process, Pid),
            {reply, ok, State0#{slot_owners := Owners0#{Pid => Ref}}};
        true ->
            {reply, {error, {already_registered, Pid}}, State0}
    end;
handle_call(#deregister_slot_owner{pid = Pid}, _From, #{slot_owners := Owners0} = State0) ->
    case maps:get(Pid, Owners0, undefined) of
        undefined ->
            {reply, ok, State0};
        Ref ->
            erlang:demonitor(Ref, [flush]),
            {reply, ok, State0#{slot_owners := maps:remove(Pid, Owners0)}}
    end;
handle_call(_Call, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info({'DOWN', _Ref, process, Pid, _Info}, #{slot_owners := Owners0} = State0) when
    is_map_key(Pid, Owners0)
->
    Owners = maps:remove(Pid, Owners0),
    ok = replayq_mem_ets_shared:purge_by_owner(Pid),
    {noreply, State0#{slot_owners := Owners}};
handle_info({'EXIT', Pid, _Reason}, #{committers := Committers0} = State0) when
    is_map_key(Pid, Committers0)
->
    {_, State} = pop_committer(State0, Pid),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

add_committer(Committers, Pid, Dir) ->
    Committers#{Pid => Dir, Dir => Pid}.

pop_committer(State0, Pid) ->
    #{committers := Committers0} = State0,
    {Dir, Committers1} = maps:take(Pid, Committers0),
    {Pid, Committers} = maps:take(Dir, Committers1),
    State = State0#{committers := Committers},
    {{Pid, Dir}, State}.

do_register_committer(State0, Dir, Pid) ->
    #{committers := Committers0} = State0,
    link(Pid),
    Committers = add_committer(Committers0, Pid, Dir),
    State0#{committers := Committers}.
