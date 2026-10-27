package io.amcp.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InMemoryEventBrokerTest {

    private InMemoryEventBroker broker;

    @BeforeEach
    void start() throws Exception {
        broker = new InMemoryEventBroker();
        broker.start().get(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void stop() throws Exception {
        broker.stop().get(5, TimeUnit.SECONDS);
    }

    @Test
    void queueSubscribersShareEventsRoundRobin() throws Exception {
        List<Event> a = Collections.synchronizedList(new ArrayList<>());
        List<Event> b = Collections.synchronizedList(new ArrayList<>());
        broker.subscribeQueue("jobs.run", "workers", a::add);
        broker.subscribeQueue("jobs.run", "workers", b::add);

        for (int i = 0; i < 4; i++) {
            broker.publish(Event.create("jobs.run", Integer.valueOf(i))).get(5, TimeUnit.SECONDS);
        }
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (a.size() + b.size() < 4 && System.nanoTime() < end) {
            Thread.sleep(20);
        }
        assertEquals(2, a.size(), "round-robin gives the first queue member half the events");
        assertEquals(2, b.size(), "round-robin gives the second queue member half the events");
    }

    @Test
    void fanOutSubscribersAllReceive() throws Exception {
        List<Event> a = Collections.synchronizedList(new ArrayList<>());
        List<Event> b = Collections.synchronizedList(new ArrayList<>());
        broker.subscribe("news.item", a::add);
        broker.subscribe("news.item", b::add);

        broker.publish(Event.create("news.item", "x")).get(5, TimeUnit.SECONDS);
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while ((a.isEmpty() || b.isEmpty()) && System.nanoTime() < end) {
            Thread.sleep(20);
        }
        assertEquals(1, a.size());
        assertEquals(1, b.size());
    }

    @Test
    void brokerCanBeRestartedAfterStop() throws Exception {
        broker.stop().get(5, TimeUnit.SECONDS);
        broker.start().get(5, TimeUnit.SECONDS);
        List<Event> got = Collections.synchronizedList(new ArrayList<>());
        broker.subscribe("again.*", got::add);
        broker.publish(Event.create("again.x", Integer.valueOf(1))).get(5, TimeUnit.SECONDS);
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (got.isEmpty() && System.nanoTime() < end) {
            Thread.sleep(20);
        }
        assertEquals(1, got.size(), "publish after stop() then start() must deliver, not throw RejectedExecutionException");
    }
}
