-module(benchmark_tests).

-include_lib("eunit/include/eunit.hrl").

-define(DIR, filename:join([data_dir(), ?FUNCTION_NAME, integer_to_list(uniq())])).
-define(WRITE_CHUNK_SIZE, 10).
-define(ITEM_BYTES, 2 bsl 10).

run_test_() ->
  Dir = ?DIR,
  Config = #{dir => Dir, seg_bytes => 100 bsl 20},
  {timeout, 60,
   fun() ->
       Writter = erlang:spawn_link(fun() -> writter(Config) end),
       Now = erlang:system_time(),
       timer:sleep(timer:seconds(10)),
       _ = erlang:send(Writter, {stop, self()}),
       Result = wait_for_result(Now),
       print_result(Result)
   end}.

wait_for_result(Then) ->
  receive
    {result, Stats} ->
      Now = erlang:system_time(),
      IntervalSeconds = (Now - Then) / 1000000000,
      Stats#{time => IntervalSeconds}
  end.

print_result(#{count := Count, bytes := Bytes, time := Seconds}) ->
  io:format(user, "~p messages per second\n~p bytes per second",
            [Count / Seconds, Bytes / Seconds]).

writter(Config) ->
  Q = replayq:open(Config),
  writter_loop(Q).

writter_loop(Q0) ->
  Q = do_write_chunk(Q0, ?WRITE_CHUNK_SIZE),
  receive
    {stop, Pid} ->
      Pid ! {result, #{bytes => replayq:bytes(Q),
                       count => replayq:count(Q)
                      }},
      exit(normal)
  after
    0 ->
      writter_loop(Q)
  end.

do_write_chunk(Q, N) ->
  Bytes = lists:duplicate(N, iolist_to_binary(lists:duplicate(?ITEM_BYTES, 0))),
  replayq:append(Q, Bytes).

data_dir() -> "./test-data".

uniq() ->
  {_, _, Micro} = erlang:timestamp(),
  Micro.

