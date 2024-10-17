-module(replayq_registry).

-behaviour(gen_server).

%% API
-export([
    start_link/0,

    register_committer/2,
    deregister_committer/1
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
-record(register_committer, {dir :: filename:filename_all(), pid :: pid()}).
-record(deregister_committer, {pid :: pid()}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

register_committer(Dir, Pid) ->
    gen_server:call(?MODULE, #register_committer{dir = Dir, pid = Pid}, infinity).

deregister_committer(Pid) ->
    gen_server:call(?MODULE, #deregister_committer{pid = Pid}, infinity).

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(_Opts) ->
    process_flag(trap_exit, true),
    State = #{committers => #{}},
    {ok, State}.

handle_call(#register_committer{dir = Dir, pid = Pid}, _From, State0) ->
    %% Should we expand the directory path to avoid tricks with links and relative paths?
    #{committers := Committers0} = State0,
    case Committers0 of
        #{Dir := SomePid} ->
            case is_process_alive(SomePid) of
                true ->
                    {reply, {error, already_registered}, State0};
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
handle_call(_Call, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

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
