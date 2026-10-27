# AMCP Kafka Broker

`KafkaEventBroker` is an `io.amcp.core.EventBroker` backed by Apache Kafka. Agents on different JVMs publish and
subscribe through the same topics.

```java
KafkaEventBroker broker = new KafkaEventBroker("127.0.0.1:9092", "mesh", "instance-1");
broker.start().get();

broker.subscribe("bee.*", event -> System.out.println(event.getTopic()));          // fan-out
broker.subscribeQueue("jobs.run", "workers", event -> runJob(event));               // competing consumers
broker.publish(Event.create("bee.heartbeat", Map.of("bee", "b1"))).get();

broker.stop().get();
```

The 4-argument constructor `(bootstrap, groupId, instanceId, java.util.Properties extra)` passes extra Kafka client
properties to the producer and to every consumer. The bootstrap servers, group ids and serializers are always set by
the broker and override any conflicting extra property.

## Delivery semantics

**Correction:** an earlier revision of this broker used Kafka auto-commit while running handlers asynchronously.
Offsets were then committed on poll, before the handler ran, which is at-most-once: a crash or a failing handler
lost the event. That was wrong and is fixed. The broker now commits manually.

- **At-least-once.** The poll thread hands a polled batch to the handlers, waits for all of them (at most 30 seconds),
  and only then commits the offsets of the records whose handlers returned normally. If a handler throws, or does not
  finish in time, that record is not committed and the partition is rewound to it, so the record (and the records after
  it in the same partition and batch) is delivered again. After 3 failed deliveries of the same record it is logged at
  ERROR and skipped, so one poison record cannot block a partition forever.
- **Handlers must be idempotent.** A record can be delivered more than once (a retry after a failure, a rebalance, a
  crash between handler completion and commit, a handler that finished after the 30-second wait). In fan-out mode a
  retry also repeats the handlers that already succeeded for that record.
- `enable.auto.commit` is always `false`; an extra property cannot turn it back on.
- Because the poll thread waits for the batch, a handler that blocks for a long time delays all later records of that
  consumer (the wait is bounded, and Kafka's `max.poll.interval.ms` default of 5 minutes is not at risk).

The broker offers two delivery modes. Choose the one that matches what the subscriber should see.

### Fan-out: `subscribe(topicPattern, handler)`

Every broker instance receives every matching event, which is the same meaning as `InMemoryEventBroker`.

- Each instance runs one consumer, in consumer group `groupId + ".fanout." + instanceId`.
- The consumer starts at the earliest offset, but it skips any event whose CloudEvent `time` is older than the
  subscription's creation time minus 5 seconds. A newly started instance therefore does not replay history.
  Events without a `time` are not filtered.
- The 5-second margin is a semantic rule about clocks, not a performance claim. If a producer's clock is more than
  about 5 seconds behind the subscriber's clock, its fresh events are skipped by this filter.

### Competing consumers: `subscribeQueue(topicPattern, queueName, handler)`

Each event is delivered to exactly one subscriber across all broker instances that share `groupId` and `queueName`.

- There is one consumer per queue name, in consumer group `groupId + ".queue." + queueName`. Different queue names
  each receive a full copy of the stream.
- The consumer starts at the earliest offset and has no history filter. A queue that is new therefore processes
  events still retained in the topic.
- Within one instance, if several subscriptions share the queue name, each event goes to one of them, round-robin.
- **Every instance that shares a `queueName` MUST subscribe the same topic patterns.** Kafka assigns partitions of
  the union of the group's topics to its members. If a record lands on an instance that has no local subscription
  matching its topic, that instance cannot handle it, and its offset is committed for the whole group, so no other
  instance sees it: the record is lost for that queue. The broker cannot repair this (Kafka groups do not allow
  handing a record back to a chosen member). It logs one WARN per (topic, queueName) pair on the first such record so
  the misconfiguration is visible. Use the same patterns everywhere, or use different queue names.
- Calling `subscribeQueue` on an `EventBroker` that does not override it (for example, a custom broker) falls back
  to fan-out `subscribe`.

### Other rules

- Handlers run on a virtual-thread executor. A slow handler does not block polling. Handler order across events is
  not guaranteed.
- `publish(event)` returns a future that completes when Kafka acknowledges the record (`acks=all`). It completes
  exceptionally on a send error, and it fails at once with `IllegalStateException` when the broker is not running.
- A subscription takes effect shortly after `subscribe` or `subscribeQueue` returns, not before: the poll thread
  applies it, and a queue subscription also waits for the group rebalance. Fan-out consumers start at the earliest
  offset and filter by event time, so recent events are still seen. Do not rely on delivery of an event that was
  published immediately before or after the call; wait for the assignment first.
- If the Kafka consumer of a worker cannot be created (for example an invalid consumer property in the 4-argument
  constructor), `start()` returns a failed future, or `subscribeQueue` throws `IllegalStateException` with the cause.
  If a subscription update fails (`subscribe` on the Kafka consumer throws), the broker logs the cause once and retries
  with bounded backoff (1 s doubling to 30 s) until it succeeds.

## Topic naming and patterns

- The Kafka topic name is the AMCP topic string, verbatim. `publish` sends `bee.heartbeat` to the topic
  `bee.heartbeat`. `subscribe("bee.heartbeat", ...)` subscribes to that same topic.
- Kafka topic names may contain only `a-z`, `A-Z`, `0-9`, `.`, `_` and `-`, up to 249 characters. Names that differ
  only by `.` versus `_` collide, and Kafka rejects the second topic created.
- Patterns are translated to regular expressions, and the broker subscribes with one `Pattern` that is the union of
  all active patterns of the consumer:

| AMCP pattern | Matches | Does not match |
|---|---|---|
| `a.b.c` | `a.b.c` | `a.b.cd`, `aXb.c` |
| `p.*` | `p.x` (exactly one segment) | `p`, `p.x.y`, `px.x` |
| `p.**` | `p`, `p.x`, `p.x.y` | `px`, `phive.x` |

- A wildcard never matches an empty segment. A topic with a trailing dot (`p.x.`) or a doubled dot (`p..x`) is matched
  only by a pattern equal to the topic, never by `p.*` or `p.**`. `Event.matchesTopic` and the Kafka regex follow the
  same rule (a table-driven test runs the same cases through both).
- Kafka subscriptions find topics by metadata refresh. The broker sets `metadata.max.age.ms=1000` (override it through
  the extra properties), so a topic created after a pattern subscription is discovered at most about 1 second later;
  the group rebalance that follows can add a second or more. Allow 2 to 5 seconds before expecting events from a
  brand-new topic. Publishing to a topic that does not exist creates it when the cluster has
  `auto.create.topics.enable=true`.

## Wire format

Records use the CloudEvents structured JSON format (`application/cloudevents+json`), through
`io.cloudevents.jackson.JsonFormat`. The record key is the CloudEvent extension `partitionkey` when present, and
otherwise the event id. A record that cannot be parsed is logged once by the consumer and skipped.

## Lifecycle

- `start()` and `stop()` are idempotent.
- `stop()` wakes and joins each poll thread, closes the consumers and the producer, and shuts the executor down. Each
  consumer is created, polled and closed by one dedicated platform thread.

## Tests

`KafkaEventBrokerIT` runs against a real broker when `AMCP_KAFKA_BOOTSTRAP` is set, for example
`AMCP_KAFKA_BOOTSTRAP=127.0.0.1:9092 mvn -pl amcp-broker-kafka verify`. Without it, the integration tests are skipped
and the build stays green. `TopicPatternsTest` and `KafkaEventBrokerUnitTest` are plain unit tests that need no broker (the latter drives the
poll logic with an in-memory consumer: redelivery, commit timing, retry, queue WARN, start failure).
