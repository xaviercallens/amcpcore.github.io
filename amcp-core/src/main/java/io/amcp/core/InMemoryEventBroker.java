package io.amcp.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * In-memory implementation of EventBroker for development and testing.
 * Events are routed directly within the JVM without external message broker.
 *
 * <p>{@link #subscribe} delivers every event to every matching subscriber (fan-out).
 * {@link #subscribeQueue} delivers each event to one subscriber per queue name,
 * chosen round-robin among the subscribers that share that queue name.
 */
public class InMemoryEventBroker implements EventBroker {
    private static final Logger logger = LoggerFactory.getLogger(InMemoryEventBroker.class);

    private final Map<String, Subscription> subscriptions = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> queueCursors = new ConcurrentHashMap<>();
    private volatile ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private volatile boolean running = false;

    private static class Subscription {
        final String topicPattern;
        final Consumer<Event> handler;
        final String queueName; // null for fan-out subscriptions

        Subscription(String topicPattern, Consumer<Event> handler, String queueName) {
            this.topicPattern = topicPattern;
            this.handler = handler;
            this.queueName = queueName;
        }
    }

    @Override
    public CompletableFuture<Void> publish(Event event) {
        if (!running) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Broker not running"));
        }

        return CompletableFuture.runAsync(() -> {
            logger.debug("Publishing event: {}", event.getTopic());

            // Fan-out subscriptions all receive the event; queue subscriptions are grouped by
            // queue name and each group receives it once, round-robin.
            Map<String, List<Subscription>> queues = new LinkedHashMap<>();
            for (Subscription sub : subscriptions.values()) {
                if (!event.matchesTopic(sub.topicPattern)) {
                    continue;
                }
                if (sub.queueName == null) {
                    deliver(sub, event);
                } else {
                    queues.computeIfAbsent(sub.queueName, k -> new ArrayList<>()).add(sub);
                }
            }
            for (Map.Entry<String, List<Subscription>> q : queues.entrySet()) {
                List<Subscription> members = q.getValue();
                int index = Math.floorMod(
                        queueCursors.computeIfAbsent(q.getKey(), k -> new AtomicInteger()).getAndIncrement(),
                        members.size());
                deliver(members.get(index), event);
            }
        }, executor);
    }

    private void deliver(Subscription sub, Event event) {
        try {
            executor.submit(() -> sub.handler.accept(event));
        } catch (Exception e) {
            logger.error("Error delivering event to subscriber: {}", e.getMessage(), e);
        }
    }

    @Override
    public String subscribe(String topicPattern, Consumer<Event> handler) {
        String subscriptionId = UUID.randomUUID().toString();
        subscriptions.put(subscriptionId, new Subscription(topicPattern, handler, null));
        logger.debug("Created subscription {} for pattern {}", subscriptionId, topicPattern);
        return subscriptionId;
    }

    @Override
    public String subscribeQueue(String topicPattern, String queueName, Consumer<Event> handler) {
        if (queueName == null || queueName.isBlank()) {
            throw new IllegalArgumentException("queueName must not be blank");
        }
        String subscriptionId = UUID.randomUUID().toString();
        subscriptions.put(subscriptionId, new Subscription(topicPattern, handler, queueName));
        logger.debug("Created queue subscription {} for pattern {} in queue {}",
                subscriptionId, topicPattern, queueName);
        return subscriptionId;
    }

    @Override
    public void unsubscribe(String subscriptionId) {
        subscriptions.remove(subscriptionId);
        logger.debug("Removed subscription {}", subscriptionId);
    }

    @Override
    public synchronized CompletableFuture<Void> start() {
        if (running) {
            return CompletableFuture.completedFuture(null);
        }

        // stop() shuts the executor down for good, so a restart needs a fresh one.
        if (executor.isShutdown()) {
            executor = Executors.newVirtualThreadPerTaskExecutor();
        }
        running = true;
        logger.info("InMemoryEventBroker started");
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> stop() {
        if (!running) {
            return CompletableFuture.completedFuture(null);
        }

        running = false;
        subscriptions.clear();
        executor.shutdown();
        logger.info("InMemoryEventBroker stopped");
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public String getBrokerType() {
        return "memory";
    }
}
