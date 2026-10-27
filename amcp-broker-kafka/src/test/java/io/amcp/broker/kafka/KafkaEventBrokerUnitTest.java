package io.amcp.broker.kafka;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.amcp.core.Event;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Deterministic unit tests for the Kafka broker's worker logic. No Kafka needed: consumers are in-memory fakes. */
class KafkaEventBrokerUnitTest {

    private KafkaEventBroker broker;
    private ListAppender<ILoggingEvent> logs;
    private Logger brokerLogger;

    @BeforeEach
    void attachLog() {
        brokerLogger = (Logger) LoggerFactory.getLogger(KafkaEventBroker.class);
        logs = new ListAppender<>();
        logs.start();
        brokerLogger.addAppender(logs);
    }

    @AfterEach
    void cleanup() throws Exception {
        brokerLogger.detachAppender(logs);
        if (broker != null) {
            broker.stop().get(15, TimeUnit.SECONDS);
        }
    }

    /** Broker whose consumers are fakes: fan-out workers get {@code fanout}, queue workers get {@code queue}. */
    private KafkaEventBroker brokerWith(FakeConsumer fanout, FakeConsumer queue) {
        KafkaEventBroker b = new KafkaEventBroker("127.0.0.1:1", "g", "i1");
        b.consumerFactory = props -> String.valueOf(props.get("group.id")).contains(".queue.") ? queue : fanout;
        b.baseRetryBackoffMs = 20;
        b.batchTimeout = Duration.ofSeconds(5);
        broker = b;
        return b;
    }

    private static void awaitTrue(BooleanSupplier c, long seconds, String what) throws InterruptedException {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (!c.getAsBoolean() && System.nanoTime() < end) {
            Thread.sleep(10);
        }
        assertTrue(c.getAsBoolean(), "timed out waiting for: " + what);
    }

    private static String json(String topic) {
        return KafkaEventBroker.encode(Event.create(topic, Map.of("n", 1)));
    }

    private long count(Level level, String fragment) {
        synchronized (logs.list) {
            return logs.list.stream()
                    .filter(e -> e.getLevel() == level && e.getFormattedMessage().contains(fragment)).count();
        }
    }

    private static String chain(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (; t != null; t = t.getCause()) {
            sb.append(t.getMessage()).append(" | ");
        }
        return sb.toString();
    }

    // ---- item 1 -------------------------------------------------------------------------------------------

    @Test
    void consumerConstructionFailureSurfacesInStart() {
        Properties bad = new Properties();
        bad.put("max.poll.records", "abc");   // consumer-only property, invalid value
        broker = new KafkaEventBroker("127.0.0.1:1", "g", "i1", bad);
        ExecutionException e = assertThrows(ExecutionException.class,
                () -> broker.start().get(30, TimeUnit.SECONDS), "start() must fail, not report success");
        assertFalse(broker.isRunning());
        assertEquals(0, broker.workerCount(), "no dead worker may stay in the map");
        assertTrue(chain(e).contains("max.poll.records"), "the cause must name the bad property: " + chain(e));
    }

    @Test
    void queueConsumerFailureSurfacesInSubscribeAndLeavesNoDeadWorker() throws Exception {
        FakeConsumer fanout = new FakeConsumer();
        broker = new KafkaEventBroker("127.0.0.1:1", "g", "i1");
        broker.consumerFactory = props -> {
            if (String.valueOf(props.get("group.id")).contains(".queue.")) {
                throw new IllegalArgumentException("queue consumer cannot be built");
            }
            return fanout;
        };
        broker.start().get(30, TimeUnit.SECONDS);
        int before = broker.workerCount();
        IllegalStateException ise = assertThrows(IllegalStateException.class,
                () -> broker.subscribeQueue("t.a", "q", e -> { }));
        assertInstanceOf(IllegalArgumentException.class, ise.getCause());
        assertEquals(before, broker.workerCount(), "the failed queue worker must not stay in the map");
    }

    // ---- item 2 -------------------------------------------------------------------------------------------

    @Test
    void failedSubscriptionUpdateIsRetriedAndLoggedOnce() throws Exception {
        FakeConsumer queue = new FakeConsumer();
        queue.failSubscribeTimes = 2;
        queue.append("t.a", json("t.a"));
        KafkaEventBroker b = brokerWith(new FakeConsumer(), queue);
        b.start().get(30, TimeUnit.SECONDS);
        List<Event> got = Collections.synchronizedList(new ArrayList<>());
        b.subscribeQueue("t.a", "q", got::add);
        awaitTrue(() -> got.size() == 1, 8, "event delivered after the subscribe failures are retried");
        assertTrue(queue.subscribeAttempts.get() >= 3, "attempts: " + queue.subscribeAttempts.get());
        assertEquals(1, count(Level.ERROR, "could not update its subscription"),
                "one failure episode is logged once, with the cause");
    }

    // ---- item 3 -------------------------------------------------------------------------------------------

    @Test
    void handlerFailureRedeliversAndCommitsOnlyAfterSuccess() throws Exception {
        FakeConsumer queue = new FakeConsumer();
        queue.append("t.a", json("t.a"));
        KafkaEventBroker b = brokerWith(new FakeConsumer(), queue);
        b.start().get(30, TimeUnit.SECONDS);
        AtomicInteger calls = new AtomicInteger();
        List<String> ids = Collections.synchronizedList(new ArrayList<>());
        b.subscribeQueue("t.a", "q", e -> {
            ids.add(e.getId());
            if (calls.incrementAndGet() == 1) {
                throw new IllegalStateException("handler killed mid-batch");
            }
        });
        awaitTrue(() -> calls.get() >= 2, 8, "redelivery after the handler threw");
        awaitTrue(() -> queue.commits.size() >= 1, 8, "commit after the successful redelivery");
        assertEquals(2, calls.get());
        assertEquals(ids.get(0), ids.get(1), "the same event is redelivered");
        assertEquals(1, queue.commits.size(), "nothing was committed for the failed attempt");
        assertEquals(1L, queue.commits.get(0).get(new TopicPartition("t.a", 0)).offset());
    }

    @Test
    void offsetsAreNotCommittedWhileAHandlerIsStillRunning() throws Exception {
        FakeConsumer queue = new FakeConsumer();
        queue.append("t.a", json("t.a"));
        KafkaEventBroker b = brokerWith(new FakeConsumer(), queue);
        b.start().get(30, TimeUnit.SECONDS);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        b.subscribeQueue("t.a", "q", e -> {
            entered.countDown();
            try {
                release.await(20, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        });
        assertTrue(entered.await(8, TimeUnit.SECONDS));
        Thread.sleep(500);
        assertTrue(queue.commits.isEmpty(), "must not commit before the handler finished (at-most-once bug)");
        release.countDown();
        awaitTrue(() -> queue.commits.size() == 1, 8, "commit after the handler completed");
    }

    @Test
    void poisonRecordIsGivenUpAfterBoundedAttemptsAndCommitted() throws Exception {
        FakeConsumer queue = new FakeConsumer();
        queue.append("t.a", json("t.a"));
        KafkaEventBroker b = brokerWith(new FakeConsumer(), queue);
        b.start().get(30, TimeUnit.SECONDS);
        AtomicInteger calls = new AtomicInteger();
        b.subscribeQueue("t.a", "q", e -> {
            calls.incrementAndGet();
            throw new IllegalStateException("always fails");
        });
        awaitTrue(() -> queue.commits.size() == 1, 10, "give-up commit");
        Thread.sleep(300);
        assertEquals(KafkaEventBroker.MAX_DELIVERY_ATTEMPTS, calls.get(), "bounded redelivery, no endless loop");
        assertEquals(1, count(Level.ERROR, "Giving up"));
    }

    // ---- item 4 -------------------------------------------------------------------------------------------

    @Test
    void recordWithoutLocalMatchWarnsOncePerTopicAndQueue() throws Exception {
        FakeConsumer queue = new FakeConsumer();
        queue.append("t.other", json("t.other"));
        queue.append("t.other", json("t.other"));
        queue.append("t.third", json("t.third"));
        queue.append("t.a", json("t.a"));
        KafkaEventBroker b = brokerWith(new FakeConsumer(), queue);
        b.start().get(30, TimeUnit.SECONDS);
        List<Event> got = Collections.synchronizedList(new ArrayList<>());
        b.subscribeQueue("t.a", "workers", got::add);
        awaitTrue(() -> got.size() == 1, 8, "the matching event is delivered");
        awaitTrue(() -> queue.commits.size() >= 1, 8, "batch committed");
        assertEquals(1, count(Level.WARN, "t.other"), "one WARN for (t.other, workers), not one per record");
        assertEquals(1, count(Level.WARN, "t.third"));
        assertTrue(logs.list.stream().anyMatch(e -> e.getLevel() == Level.WARN
                && e.getFormattedMessage().contains("workers") && e.getFormattedMessage().contains("t.other")));
    }
}
