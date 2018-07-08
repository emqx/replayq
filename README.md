[![Build Status](https://travis-ci.org/spring2maz/replayq.svg?branch=master)](https://travis-ci.org/spring2maz/replayq) [![Coverage Status](https://coveralls.io/repos/github/spring2maz/replayq/badge.svg?branch=master)](https://coveralls.io/github/spring2maz/replayq?branch=master)

# ReplayQ

A Disk Queue for Log Replay in Erlang

## Features

* Queue items (`binary()`) are written to segment files on disk to servive restart.
* An `ack/2` API is provided to record the reader position within a segment.

## Usage Example

```
Q0 = replayq:open(#{dir => "/tmp/replayq-test", seg_bytes => 10000000}),
Q1 = replayq:append(Q0, [Binary1, Binary2]),
{Q2, AckRef, [Binary1]} = replayq:pop(Q1, #{count_limit => 1}),
ok = replayq:ack(Q2, AckRef).
```
