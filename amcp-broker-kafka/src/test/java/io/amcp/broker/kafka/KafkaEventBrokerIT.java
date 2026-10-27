package io.amcp.broker.kafka;

import io.amcp.core.Event;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests against a real Kafka broker.
 *
 * <p>Set {@code AMCP_KAFKA_BOOTSTRAP} (for example {@code 127.0.0.1:9092}) to run them; without it they are skipped,
 * so a plain {@code mvn test} stays green on machines without Kafka. Every test uses its own random topic prefix
 * and consumer groups, so tests do not see each other's events.
 */
class KafkaEventBrokerIT {

    private static String bootstrap;
    private final List<KafkaEventBroker> brokers = new ArrayList<>();
    private final String prefix = "amcpit" + UUID.randomUUID().toString().replace("-", "").substring(0, 10);

    @BeforeAll
    static void requireKafka() {
        bootstrap = System.getenv("AMCP_KAFKA_BOOTSTRAP");
        assumeTrue(bootstrap != null && !bootstrap.isBlank(), "AMCP_KAFKA_BOOTSTRAP not set: Kafka integration tests skipped");
        String[] hp = bootstrap.split(",")[0].split(":");
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(hp[0], Integer.parseInt(hp[1])), 2000);
        } catch (Exception e) {
            assumeTrue(false, "Kafka not reachable at " + bootstrap);
        }
    }

    @AfterEach
    void stopBrokers() throws Exception {
        for (KafkaEventBroker b : brokers) {
            b.stop().get(10, TimeUnit.SECONDS);
        }
    }

    private KafkaEventBroker broker(String group, String instance) throws Exception {
        KafkaEventBroker b = new KafkaEventBroker(bootstrap, prefix + "-" + group, instance);
        b.start().get(20, TimeUnit.SECONDS);
        brokers.add(b);
        return b;
    }

    private static List<Event> sink() {
        return Collections.synchronizedList(new ArrayList<>());
    }

    private static void await(List<?> got, int n, long seconds) throws InterruptedException {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (got.size() < n && System.nanoTime() < end) {
            Thread.sleep(50);
        }
    }

    private static void createTopic(String bootstrapServers, String topic, int partitions) throws Exception {
        Map<String, Object> config = Map.of("bootstrap.servers", bootstrapServers);
        try (Admin admin = Admin.create(config)) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get(30, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw e;
            }
        }
    }

    @Test
    void roundTripKeepsTopicAndPayload() throws Exception {
        KafkaEventBroker b = broker("g", "i1");
        List<Event> got = sink();
        String topic = prefix + ".bee.heartbeat";
        b.subscribe(topic, got::add);
        b.publish(Event.create(topic, Map.of("bee", "b1", "load", 2))).get(10, TimeUnit.SECONDS);
        await(got, 1, 30);
        assertEquals(1, got.size(), "event delivered once");
        assertEquals(topic, got.get(0).getTopic(), "topic survives the round trip");
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = got.get(0).getPayload(Map.class);
        assertEquals("b1", payload.get("bee"));
        assertEquals(2, ((Number) payload.get("load")).intValue());
    }

    @Test
    void severalSubscriptionsOnOneBrokerAllReceive() throws Exception {
        KafkaEventBroker b = broker("g", "i1");
        List<Event> a = sink();
        List<Event> c = sink();
        b.subscribe(prefix + ".task.request", a::add);
        b.subscribe(prefix + ".task.result", c::add);
        b.publish(Event.create(prefix + ".task.request", Map.of("id", 1))).get(10, TimeUnit.SECONDS);
        b.publish(Event.create(prefix + ".task.result", Map.of("id", 1))).get(10, TimeUnit.SECONDS);
        await(a, 1, 30);
        await(c, 1, 30);
        assertEquals(1, a.size(), "first subscription still receives after a second subscribe");
        assertEquals(1, c.size(), "second subscription receives");
    }

    @Test
    void singleLevelWildcardMatchesOneLevelOnly() throws Exception {
        KafkaEventBroker b = broker("g", "i1");
        List<Event> got = sink();
        b.subscribe(prefix + ".bee.*", got::add);
        b.publish(Event.create(prefix + ".bee.alpha", Map.of("n", 1))).get(10, TimeUnit.SECONDS);
        b.publish(Event.create(prefix + ".bee.beta", Map.of("n", 2))).get(10, TimeUnit.SECONDS);
        b.publish(Event.create(prefix + ".bee.alpha.deep", Map.of("n", 3))).get(10, TimeUnit.SECONDS);
        await(got, 2, 30);
        Thread.sleep(2000); // give a wrongly matched third event time to arrive
        List<String> topics = got.stream().map(Event::getTopic).sorted().toList();
        assertEquals(List.of(prefix + ".bee.alpha", prefix + ".bee.beta"), topics);
    }

    @Test
    void fanOutReachesEveryBrokerInstance() throws Exception {
        KafkaEventBroker b1 = broker("shared", "i1");
        KafkaEventBroker b2 = broker("shared", "i2");
        List<Event> g1 = sink();
        List<Event> g2 = sink();
        String topic = prefix + ".host.announce";
        b1.subscribe(topic, g1::add);
        b2.subscribe(topic, g2::add);
        b1.publish(Event.create(topic, Map.of("hello", "mesh"))).get(10, TimeUnit.SECONDS);
        await(g1, 1, 30);
        await(g2, 1, 30);
        assertEquals(1, g1.size(), "instance 1 receives a broadcast");
        assertEquals(1, g2.size(), "instance 2 receives the same broadcast");
    }

    @Test
    void publishCompletesOnlyAfterTheBrokerAcknowledges() throws Exception {
        KafkaEventBroker b = broker("g", "i1");
        var f = b.publish(Event.create(prefix + ".ack", Map.of("x", 1)));
        f.get(10, TimeUnit.SECONDS);
        assertTrue(f.isDone() && !f.isCompletedExceptionally());
    }

    @Test
    void queueDeliversEachEventOnceAcrossInstances() throws Exception {
        String topic = prefix + ".jobs.run";
        createTopic(bootstrap, topic, 2);
        KafkaEventBroker b1 = broker("qg", "i1");
        KafkaEventBroker b2 = broker("qg", "i2");
        List<String> ids1 = Collections.synchronizedList(new ArrayList<>());
        List<String> ids2 = Collections.synchronizedList(new ArrayList<>());
        b1.subscribeQueue(topic, "workers", e -> ids1.add(e.getId()));
        b2.subscribeQueue(topic, "workers", e -> ids2.add(e.getId()));
        assertTrue(b1.awaitQueueAssignment("workers", Duration.ofSeconds(60)), "instance 1 gets a partition");
        assertTrue(b2.awaitQueueAssignment("workers", Duration.ofSeconds(60)), "instance 2 gets a partition");

        List<CompletableFuture<Void>> sent = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            sent.add(b1.publish(Event.create(topic, Map.of("n", i))));
        }
        CompletableFuture.allOf(sent.toArray(new CompletableFuture[0])).get(60, TimeUnit.SECONDS);

        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (ids1.size() + ids2.size() < 50 && System.nanoTime() < end) {
            Thread.sleep(50);
        }
        Thread.sleep(1000); // allow a duplicate delivery to show up before counting
        Set<String> union = new HashSet<>(ids1);
        union.addAll(ids2);
        assertEquals(50, ids1.size() + ids2.size(), "every event delivered exactly once in total");
        assertEquals(50, union.size(), "no event id delivered twice");
        assertTrue(!ids1.isEmpty(), "instance 1 received at least one event");
        assertTrue(!ids2.isEmpty(), "instance 2 received at least one event");
    }

    @Test
    void topicCreatedAfterSubscribeIsDelivered() throws Exception {
        KafkaEventBroker b = broker("g", "i1");
        List<Event> got = sink();
        String topic = prefix + ".late.topic";
        b.subscribe(prefix + ".late.*", got::add);
        Thread.sleep(3000); // the pattern subscription is active before the topic exists
        createTopic(bootstrap, topic, 1);
        Thread.sleep(3000); // metadata refresh runs every 1 s
        b.publish(Event.create(topic, Map.of("late", true))).get(20, TimeUnit.SECONDS);
        await(got, 1, 60);
        assertEquals(1, got.size(), "event on a topic created after subscribe is delivered");
        assertEquals(topic, got.get(0).getTopic());
    }

    @Test
    void newInstanceDoesNotReplayOldHistory() throws Exception {
        String topic = prefix + ".hist.event";
        KafkaEventBroker publisher = broker("hg", "old");
        Event old = Event.create(topic, Map.of("when", "old"));
        publisher.publish(old).get(20, TimeUnit.SECONDS);
        Thread.sleep(7000);

        KafkaEventBroker fresh = broker("hg", "new");
        List<Event> got = sink();
        fresh.subscribe(topic, got::add);
        Event recent = Event.create(topic, Map.of("when", "new"));
        fresh.publish(recent).get(20, TimeUnit.SECONDS);
        await(got, 1, 60);
        Thread.sleep(2000); // the old event would have arrived before the new one if it were replayed

        List<String> ids = got.stream().map(Event::getId).toList();
        assertEquals(List.of(recent.getId()), ids, "history older than 5 s before subscribe is not replayed");
    }

    @Test
    void doubleStarRespectsSegmentBoundary() throws Exception {
        KafkaEventBroker b = broker("g", "i1");
        List<Event> got = sink();
        b.subscribe(prefix + ".p.**", got::add);
        for (String t : List.of(prefix + ".p", prefix + ".p.x", prefix + ".px", prefix + ".p.x.y")) {
            b.publish(Event.create(t, Map.of("t", t))).get(10, TimeUnit.SECONDS);
        }
        await(got, 3, 60);
        Thread.sleep(2000); // give a wrongly matched "px" event time to arrive
        List<String> topics = got.stream().map(Event::getTopic).sorted().toList();
        assertEquals(List.of(prefix + ".p", prefix + ".p.x", prefix + ".p.x.y"), topics,
                "p.** matches p and p.<anything>, never px");
    }
}
