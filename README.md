# ReplayQ

A Disk Queue for Log Replay in Erlang

## Features

* Queue items are written to segment files on disk to survive restart.
* Batch popping items out of queue with size/count limit.
* An `ack/2` API is provided to record the reader position within a segment.
* Add config option `max_total_bytes` to limit the total size of replayq logs

## Usage Example

### Mem Only

```erlang
Q0 = replayq:open(#{mem_only => true}),
Q1 = replayq:append(Q0, [Binary1, Binary2]),
{Q2, AckRef, [Binary1]} = replayq:pop(Q1, #{bytes_limt => 1}),
ok = replayq:ack(Q2, AckRef).
```

### Binary Queue Items

```erlang
Q0 = replayq:open(#{dir => "/tmp/replayq-test", seg_bytes => 10000000}),
Q1 = replayq:append(Q0, [Binary1, Binary2]),
{Q2, AckRef, [Binary1]} = replayq:pop(Q1, #{count_limit => 1}),
ok = replayq:ack(Q2, AckRef).
```

### User Defined Queue Items

```erlang
Q0 = replayq:open(#{dir => "/tmp/replayq-test",
                    seg_bytes => 10000000,
                    sizer => fun({K, V}) -> size(K) + size(V) end,
                    marshaller => fun({K, V}) -> term_to_binary({K, V});
                                     (Bin)    -> binary_to_term(Bin)
                                  end
                   }),
Q1 = replayq:append(Q0, [{<<"k1">>, <<"v1">>}, {<<"k2">>, <<"v2">>}]),
{Q2, AckRef, [{<<"k1">>, <<"v1">>}]} = replayq:pop(Q1, #{count_limit => 1}),
ok = replayq:ack(Q2, AckRef).
```

### User Defined Stop Before Function

In this example, we want to create a batch of items such that all items in the
batch are the same. We stop adding items to the batch as soon as we encounter
an item that differs from the first item in the batch.

```erlang
Q0 = replayq:open(#{mem_only => true}),
Q1 = replayq:append(Q0,
                    [
                     <<"type1">>,
                     <<"type1">>,
                     <<"type2">>,
                     <<"type2">>,
                     <<"type2">>,
                     <<"type3">>
                    ]),
StopBeforeFunc =
fun(Item, #{current_type := none}) ->
        #{current_type => Item};
   (Item, #{current_type := Item}) ->
        %% Item and current_type are the same
        #{current_type => Item};
   (Item, #{current_type := _OtherType}) ->
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
ok = replayq:ack(Q5, AckRef4).
```

### Offload mode

In offload mode, the disk queue is only used to offload queue tail segments.
Add `offload => true` to `Config` for `replayq:open/1`.

#### Volatile mode

Using `offload => {true, volatile}` in the `Config` for
`replayq:open/1` will make it unconditionally clear any previous
segments and commit file in the given work directory.  Also, it will
not dump in-memory data back to the disk when closing.
