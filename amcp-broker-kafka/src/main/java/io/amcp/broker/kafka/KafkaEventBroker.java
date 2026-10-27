package io.amcp.broker.kafka;

import io.amcp.core.Event;
import io.amcp.core.EventBroker;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Kafka-based EventBroker for a distributed AMCP mesh.
 *
 * <p>Topic names are the AMCP topic strings verbatim. Two delivery semantics are offered:
 * <ul>
 *   <li>{@link #subscribe} is fan-out: every broker instance receives every matching event.
 *       Each instance runs one consumer with group {@code groupId + ".fanout." + instanceId}.
 *       Events older than the subscription's creation time minus 5 seconds are skipped, so a new
 *       instance does not replay history.</li>
 *   <li>{@link #subscribeQueue} is competing consumers: each event goes to one subscriber across
 *       all instances that share {@code groupId} and {@code queueName}. The consumer group is
 *       {@code groupId + ".queue." + queueName}; there is no history filter.</li>
 * </ul>
 *
 * <p>Delivery is at-least-once: offsets are committed manually, after the handlers of a polled batch have
 * completed (bounded wait). A failed or unfinished record is rewound and redelivered, at most
 * {@link #MAX_DELIVERY_ATTEMPTS} times. Handlers must be idempotent. In queue mode every instance sharing a
 * {@code queueName} must subscribe the same patterns; otherwise a record can be committed without any handler.
 *
 * <p>Threading: every {@link KafkaConsumer} is created, polled and closed by one dedicated platform
 * thread. Subscribe and unsubscribe calls enqueue changes and wake that thread. Handlers run on a
 * virtual-thread executor, so a slow handler never blocks polling.
 */
public class KafkaEventBroker implements EventBroker {
    private static final Logger logger = LoggerFactory.getLogger(KafkaEventBroker.class);

    /** Lookback applied to fan-out subscriptions: events older than (creation - 5 s) are skipped. */
    static final Duration FANOUT_HISTORY_GRACE = Duration.ofSeconds(5);

    /** A record whose handler keeps failing is delivered at most this many times, then skipped with an ERROR log. */
    static final int MAX_DELIVERY_ATTEMPTS = 3;

    private static final String FANOUT_KEY = "fanout";
    private static final String QUEUE_KEY_PREFIX = "queue:";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);
    private static final long IDLE_WAIT_MS = 500;
    private static final long STOP_JOIN_MS = 10_000;
    private static final long ERROR_BACKOFF_MS = 1_000;
    private static final long MAX_RETRY_BACKOFF_MS = 30_000;
    private static final long CONSUMER_CREATE_TIMEOUT_S = 30;
    /** metadata.max.age.ms set on every consumer (extra properties may override it). */
    static final int METADATA_MAX_AGE_MS = 1000;
    private static final String CLOUDEVENTS_JSON = "application/cloudevents+json";

    private static final EventFormat JSON_FORMAT = Objects.requireNonNull(
            EventFormatProvider.getInstance().resolveFormat(CLOUDEVENTS_JSON),
            "CloudEvents JSON format not found on the classpath (cloudevents-json-jackson)");

    private final String bootstrapServers;
    private final String groupId;
    private final String instanceId;
    private final Properties extraProperties;

    /** Guards start, stop, subscribe, unsubscribe and worker creation. Never taken by publish(). */
    private final Object lifecycle = new Object();
    private final Map<String, Subscription> subscriptions = new ConcurrentHashMap<>();
    private final Map<String, PollWorker> workers = new ConcurrentHashMap<>();
    private volatile KafkaProducer<String, String> producer;
    private volatile ExecutorService executor;
    private volatile boolean running = false;

    // Test seams (package-private): replaced in unit tests, never by production code.
    volatile java.util.function.Function<Properties, org.apache.kafka.clients.consumer.Consumer<String, String>> consumerFactory =
            props -> new KafkaConsumer<>(props);
    volatile Duration batchTimeout = Duration.ofSeconds(30);
    volatile long baseRetryBackoffMs = 1_000;

    /** One subscription as registered by the caller. Immutable apart from the poll thread's own copy. */
    private static final class Subscription {
        final String id;
        final String topicPattern;
        final Consumer<Event> handler;
        final String queueName;   // null for fan-out
        final Instant notBefore;  // null for queue subscriptions (no history filter)

        Subscription(String id, String topicPattern, Consumer<Event> handler, String queueName, Instant notBefore) {
            this.id = id;
            this.topicPattern = topicPattern;
            this.handler = handler;
            this.queueName = queueName;
            this.notBefore = notBefore;
        }

        String workerKey() {
            return queueName == null ? FANOUT_KEY : QUEUE_KEY_PREFIX + queueName;
        }

        boolean accepts(Event event) {
            if (!event.matchesTopic(topicPattern)) {
                return false;
            }
            if (notBefore == null) {
                return true;
            }
            OffsetDateTime time = event.getTime();
            return time == null || !time.toInstant().isBefore(notBefore);
        }
    }

    private record Change(Subscription subscription, boolean add) {
    }

    /**
     * Owns one KafkaConsumer and the thread that drives it. Fields marked "poll thread only" are
     * touched only by that thread.
     */
    private final class PollWorker implements Runnable {
        private final String name;
        private final String consumerGroup;
        private final boolean queue;
        private final Thread thread;
        /** Completed once the consumer exists, or completed exceptionally when it could not be created. */
        private final CompletableFuture<Void> ready = new CompletableFuture<>();
        private final ConcurrentLinkedQueue<Change> changes = new ConcurrentLinkedQueue<>();
        private final Object monitor = new Object();
        private final Map<String, Subscription> active = new LinkedHashMap<>();   // poll thread only
        private final AtomicInteger roundRobin = new AtomicInteger();
        private final Set<String> warnedDrops = new HashSet<>();                  // poll thread only
        private final Map<String, Integer> failedAttempts = new HashMap<>();      // poll thread only
        private Set<String> appliedPatterns = new TreeSet<>();                    // poll thread only
        private int applyFailures = 0;                                            // poll thread only
        private long applyRetryAtNanos = 0;                                       // poll thread only
        private boolean applyFailureLogged = false;                               // poll thread only
        private volatile org.apache.kafka.clients.consumer.Consumer<String, String> consumer;
        private volatile boolean stopping = false;
        private volatile int assignedPartitions = 0;

        PollWorker(String name, String consumerGroup, boolean queue) {
            this.name = name;
            this.consumerGroup = consumerGroup;
            this.queue = queue;
            this.thread = new Thread(this, "amcp-kafka-" + name);
            this.thread.setDaemon(true);
        }

        void enqueue(Change change) {
            changes.add(change);
            synchronized (monitor) {
                monitor.notifyAll();
            }
            wakeConsumer();
        }

        void requestStop() {
            stopping = true;
            synchronized (monitor) {
                monitor.notifyAll();
            }
            wakeConsumer();
        }

        private void wakeConsumer() {
            org.apache.kafka.clients.consumer.Consumer<String, String> c = consumer;
            if (c != null) {
                try {
                    c.wakeup();
                } catch (RuntimeException e) {
                    logger.debug("wakeup on consumer {} ignored: {}", name, e.getMessage());
                }
            }
        }

        void awaitTermination(long timeoutMs) {
            try {
                thread.join(timeoutMs);
                if (thread.isAlive()) {
                    logger.warn("Kafka poll thread {} did not stop within {} ms", name, timeoutMs);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void run() {
            org.apache.kafka.clients.consumer.Consumer<String, String> c;
            try {
                c = consumerFactory.apply(consumerProperties(consumerGroup));
            } catch (RuntimeException | Error e) {
                logger.error("Could not create Kafka consumer {}: {}", name, e.getMessage(), e);
                workers.remove(name, this);
                ready.completeExceptionally(e);
                return;
            }
            consumer = c;
            logger.info("Kafka consumer {} created in group {}", name, consumerGroup);
            ready.complete(null);
            try {
                while (!stopping) {
                    try {
                        applyChanges(c);
                        if (active.isEmpty() || appliedPatterns.isEmpty()) {
                            // Nothing is subscribed (or the last update failed and is waiting for its retry):
                            // never call poll() on a consumer without a subscription.
                            idle();
                            continue;
                        }
                        ConsumerRecords<String, String> records = c.poll(POLL_TIMEOUT);
                        assignedPartitions = c.assignment().size();
                        if (!records.isEmpty()) {
                            processBatch(c, records);
                        }
                    } catch (WakeupException expected) {
                        // Raised on purpose by wakeup(): a change is queued or stop was requested.
                    } catch (RuntimeException e) {
                        if (stopping) {
                            break;
                        }
                        logger.error("Kafka poll loop {} error: {}", name, e.getMessage());
                        sleepMs(ERROR_BACKOFF_MS);
                    }
                }
            } finally {
                // Never leave a dead worker in the map: a later subscribe must create a fresh one.
                workers.remove(name, this);
                consumer = null;
                try {
                    c.close(Duration.ofSeconds(5));
                } catch (RuntimeException e) {
                    logger.warn("Error closing Kafka consumer {}: {}", name, e.getMessage());
                }
                logger.info("Kafka consumer {} closed", name);
            }
        }

        /**
         * Drains queued changes and resubscribes only when the set of patterns actually changed. The applied set
         * is updated only after the Kafka call succeeded; on failure the update is retried with bounded backoff.
         */
        private void applyChanges(org.apache.kafka.clients.consumer.Consumer<String, String> c) {
            Change change;
            while ((change = changes.poll()) != null) {
                if (change.add()) {
                    active.put(change.subscription().id, change.subscription());
                } else {
                    active.remove(change.subscription().id);
                }
            }
            Set<String> wanted = new TreeSet<>();
            for (Subscription s : active.values()) {
                wanted.add(s.topicPattern);
            }
            if (wanted.equals(appliedPatterns)) {
                return;
            }
            if (applyFailures > 0 && System.nanoTime() - applyRetryAtNanos < 0) {
                return;   // still backing off after a failed update
            }
            try {
                if (wanted.isEmpty()) {
                    c.unsubscribe();
                    logger.debug("Consumer {} unsubscribed (no active subscriptions)", name);
                } else {
                    c.subscribe(TopicPatterns.union(wanted));
                    logger.debug("Consumer {} subscribed to {}", name, wanted);
                }
                appliedPatterns = wanted;
                applyFailures = 0;
                applyFailureLogged = false;
            } catch (RuntimeException e) {
                applyFailures++;
                long backoffMs = Math.min(MAX_RETRY_BACKOFF_MS,
                        baseRetryBackoffMs << Math.min(applyFailures - 1, 10));
                applyRetryAtNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(backoffMs);
                if (!applyFailureLogged) {
                    applyFailureLogged = true;
                    logger.error("Consumer {} could not update its subscription to {}; the subscription stays "
                            + "inactive and the update is retried with backoff: {}", name, wanted, e.toString(), e);
                }
            }
        }

        /** Waits without touching the consumer; used when nothing is applied, so poll() is never called. */
        private void idle() {
            long waitMs = IDLE_WAIT_MS;
            if (applyFailures > 0) {
                long untilRetry = TimeUnit.NANOSECONDS.toMillis(applyRetryAtNanos - System.nanoTime());
                waitMs = Math.max(1, Math.min(waitMs, untilRetry));
            }
            synchronized (monitor) {
                if (changes.isEmpty() && !stopping) {
                    try {
                        monitor.wait(waitMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        stopping = true;
                    }
                }
            }
        }

        private void sleepMs(long ms) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                stopping = true;
            }
        }

        private record Dispatched(ConsumerRecord<String, String> record, CompletableFuture<Void> done) {
        }

        /**
         * Hands every record of the polled batch to the handlers, waits (bounded) for all of them, and commits
         * only the offsets whose handlers completed. A failed or unfinished record is not committed and the
         * partition is rewound to it, so it is redelivered: at-least-once. After {@link #MAX_DELIVERY_ATTEMPTS}
         * failed deliveries a record is logged and skipped so one poison record cannot block a partition.
         */
        private void processBatch(org.apache.kafka.clients.consumer.Consumer<String, String> c,
                                  ConsumerRecords<String, String> records) {
            Map<TopicPartition, List<Dispatched>> byPartition = new LinkedHashMap<>();
            List<CompletableFuture<Void>> all = new ArrayList<>();
            for (TopicPartition tp : records.partitions()) {
                List<Dispatched> list = new ArrayList<>();
                for (ConsumerRecord<String, String> record : records.records(tp)) {
                    CompletableFuture<Void> done = dispatch(record);
                    list.add(new Dispatched(record, done));
                    all.add(done);
                }
                byPartition.put(tp, list);
            }
            if (!awaitAll(all)) {
                return;   // stopping: commit nothing, the batch is redelivered on the next start
            }

            Map<TopicPartition, OffsetAndMetadata> commit = new LinkedHashMap<>();
            Map<TopicPartition, Long> rewind = new LinkedHashMap<>();
            for (Map.Entry<TopicPartition, List<Dispatched>> entry : byPartition.entrySet()) {
                TopicPartition tp = entry.getKey();
                boolean blocked = false;
                for (Dispatched d : entry.getValue()) {
                    ConsumerRecord<String, String> r = d.record();
                    String key = r.topic() + "/" + r.partition() + "/" + r.offset();
                    boolean ok = d.done().isDone() && !d.done().isCompletedExceptionally();
                    if (ok) {
                        failedAttempts.remove(key);
                    } else {
                        int attempts = failedAttempts.merge(key, 1, Integer::sum);
                        if (attempts >= MAX_DELIVERY_ATTEMPTS) {
                            logger.error("Giving up on record {} after {} failed delivery attempts; it is skipped "
                                    + "and its offset is committed", key, attempts);
                            failedAttempts.remove(key);
                            ok = true;
                        }
                    }
                    if (blocked) {
                        continue;
                    }
                    if (ok) {
                        commit.put(tp, new OffsetAndMetadata(r.offset() + 1));
                    } else {
                        blocked = true;
                        rewind.put(tp, r.offset());
                    }
                }
            }
            commitOffsets(c, commit);
            if (!rewind.isEmpty()) {
                for (Map.Entry<TopicPartition, Long> e : rewind.entrySet()) {
                    c.seek(e.getKey(), e.getValue());
                }
                logger.warn("Consumer {} will redeliver unfinished or failed records from {}", name, rewind);
                sleepMs(baseRetryBackoffMs);
            }
        }

        private void commitOffsets(org.apache.kafka.clients.consumer.Consumer<String, String> c,
                                   Map<TopicPartition, OffsetAndMetadata> commit) {
            if (commit.isEmpty()) {
                return;
            }
            for (int attempt = 0; attempt < 3; attempt++) {
                try {
                    c.commitSync(commit);
                    return;
                } catch (WakeupException wakeup) {
                    // A subscribe/unsubscribe woke the consumer while we were committing: try again.
                } catch (RuntimeException e) {
                    logger.warn("Consumer {} could not commit offsets {} (the records may be redelivered): {}",
                            name, commit, e.toString());
                    return;
                }
            }
            logger.warn("Consumer {} could not commit offsets {} after repeated wakeups", name, commit);
        }

        /** Waits for all handler futures, at most {@code batchTimeout}. Returns false when the worker is stopping. */
        private boolean awaitAll(List<CompletableFuture<Void>> futures) {
            CompletableFuture<Void> all = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            long deadline = System.nanoTime() + batchTimeout.toNanos();
            while (!all.isDone()) {
                if (stopping) {
                    return false;
                }
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    logger.warn("Consumer {}: handlers did not finish within {} ms; unfinished records are not "
                            + "committed and will be redelivered", name, batchTimeout.toMillis());
                    return true;
                }
                try {
                    all.get(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(100)), TimeUnit.NANOSECONDS);
                } catch (TimeoutException | ExecutionException ignored) {
                    // re-check state; failures are read per record afterwards
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    stopping = true;
                    return false;
                }
            }
            return true;
        }

        /**
         * Routes one record. Runs on the poll thread and only hands work to the executor. The returned future
         * completes normally when every handler it was given returned normally (or nobody had to handle it).
         */
        private CompletableFuture<Void> dispatch(ConsumerRecord<String, String> record) {
            Event event;
            try {
                event = decode(record.value());
            } catch (RuntimeException e) {
                logger.warn("Skipping malformed record on topic {} at offset {}: {}",
                        record.topic(), record.offset(), e.getMessage());
                return CompletableFuture.completedFuture(null);
            }
            List<Subscription> matches = new ArrayList<>();
            for (Subscription s : active.values()) {
                if (s.accepts(event)) {
                    matches.add(s);
                }
            }
            if (!queue) {
                List<CompletableFuture<Void>> futures = new ArrayList<>();
                for (Subscription s : matches) {
                    futures.add(deliver(s, event));
                }
                return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            }
            if (matches.isEmpty()) {
                // The group assigned us a partition, but no local subscription matches. The offset is committed
                // with the batch, so no other instance will see this record: all instances sharing a queue name
                // must subscribe the same patterns.
                if (warnedDrops.add(record.topic())) {
                    logger.warn("Queue '{}' dropped a record on topic '{}': this instance has no local subscription "
                            + "that matches it, and the offset is committed for the whole queue group. Every "
                            + "instance sharing a queue name must subscribe the same topic patterns.",
                            queueNameOf(), record.topic());
                }
                return CompletableFuture.completedFuture(null);
            }
            return deliver(matches.get(Math.floorMod(roundRobin.getAndIncrement(), matches.size())), event);
        }

        private String queueNameOf() {
            return name.startsWith(QUEUE_KEY_PREFIX) ? name.substring(QUEUE_KEY_PREFIX.length()) : name;
        }

        private CompletableFuture<Void> deliver(Subscription s, Event event) {
            CompletableFuture<Void> done = new CompletableFuture<>();
            ExecutorService ex = executor;
            if (ex == null) {
                done.completeExceptionally(new IllegalStateException("broker is stopping"));
                return done;
            }
            try {
                ex.execute(() -> {
                    try {
                        s.handler.accept(event);
                        done.complete(null);
                    } catch (Throwable t) {
                        logger.error("Handler for {} failed on event {}: {}",
                                event.getTopic(), event.getId(), t.getMessage(), t);
                        done.completeExceptionally(t);
                        if (t instanceof Error err) {
                            throw err;
                        }
                    }
                });
            } catch (RejectedExecutionException e) {
                logger.warn("Not delivering event {} because the broker is stopping", event.getId());
                done.completeExceptionally(e);
            }
            return done;
        }
    }

    /**
     * Creates a new KafkaEventBroker.
     *
     * @param bootstrapServers Kafka bootstrap servers (e.g., "localhost:9092")
     * @param groupId Consumer group base ID shared by the mesh
     * @param instanceId Unique instance identifier
     */
    public KafkaEventBroker(String bootstrapServers, String groupId, String instanceId) {
        this(bootstrapServers, groupId, instanceId, new Properties());
    }

    /**
     * Creates a new KafkaEventBroker with extra Kafka client properties.
     *
     * <p>The extra properties apply to the producer and to every consumer. Connection and identity
     * settings (bootstrap servers, group id, serializers) are always set by the broker and win over
     * the extra properties.
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @param groupId Consumer group base ID shared by the mesh
     * @param instanceId Unique instance identifier
     * @param extraProperties additional Kafka client properties (may be null)
     */
    public KafkaEventBroker(String bootstrapServers, String groupId, String instanceId, Properties extraProperties) {
        this.bootstrapServers = Objects.requireNonNull(bootstrapServers, "bootstrapServers");
        this.groupId = Objects.requireNonNull(groupId, "groupId");
        this.instanceId = Objects.requireNonNull(instanceId, "instanceId");
        this.extraProperties = new Properties();
        if (extraProperties != null) {
            this.extraProperties.putAll(extraProperties);
        }
    }

    @Override
    public CompletableFuture<Void> publish(Event event) {
        KafkaProducer<String, String> p = producer;
        ExecutorService ex = executor;
        if (!running || p == null || ex == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("Broker not running"));
        }
        CompletableFuture<Void> acknowledged = new CompletableFuture<>();
        try {
            ex.execute(() -> send(p, event, acknowledged));
        } catch (RejectedExecutionException e) {
            acknowledged.completeExceptionally(new IllegalStateException("Broker not running", e));
        }
        return acknowledged;
    }

    private void send(KafkaProducer<String, String> p, Event event, CompletableFuture<Void> acknowledged) {
        try {
            String topic = event.getTopic();
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, recordKey(event), encode(event));
            p.send(record, (metadata, error) -> {
                if (error != null) {
                    logger.error("Error publishing to topic {}: {}", topic, error.getMessage());
                    acknowledged.completeExceptionally(error);
                } else {
                    logger.debug("Published to topic {} partition {} offset {}",
                            topic, metadata.partition(), metadata.offset());
                    acknowledged.complete(null);
                }
            });
        } catch (RuntimeException e) {
            logger.error("Error publishing event {}: {}", event.getId(), e.getMessage(), e);
            acknowledged.completeExceptionally(e);
        }
    }

    /** Record key: the CloudEvent "partitionkey" extension when present, otherwise the event id. */
    private static String recordKey(Event event) {
        Object partitionKey = event.getCloudEvent().getExtension("partitionkey");
        return partitionKey != null ? partitionKey.toString() : event.getId();
    }

    @Override
    public String subscribe(String topicPattern, Consumer<Event> handler) {
        Instant notBefore = Instant.now().minus(FANOUT_HISTORY_GRACE);
        return register(new Subscription(UUID.randomUUID().toString(), topicPattern, handler, null, notBefore));
    }

    @Override
    public String subscribeQueue(String topicPattern, String queueName, Consumer<Event> handler) {
        if (queueName == null || queueName.isBlank()) {
            throw new IllegalArgumentException("queueName must not be blank");
        }
        return register(new Subscription(UUID.randomUUID().toString(), topicPattern, handler, queueName, null));
    }

    private String register(Subscription subscription) {
        synchronized (lifecycle) {
            // Create the worker first: if its consumer cannot be built this throws and nothing is registered.
            PollWorker worker = running ? workerFor(subscription) : null;
            subscriptions.put(subscription.id, subscription);
            if (worker != null) {
                worker.enqueue(new Change(subscription, true));
            }
        }
        logger.debug("Registered subscription {} for pattern {} (queue {})",
                subscription.id, subscription.topicPattern, subscription.queueName);
        return subscription.id;
    }

    @Override
    public void unsubscribe(String subscriptionId) {
        synchronized (lifecycle) {
            Subscription subscription = subscriptions.remove(subscriptionId);
            if (subscription != null && running) {
                PollWorker worker = workers.get(subscription.workerKey());
                if (worker != null) {
                    worker.enqueue(new Change(subscription, false));
                }
            }
        }
        logger.debug("Removed subscription: {}", subscriptionId);
    }

    /** Must be called while holding {@code lifecycle} and while the broker is running. */
    private PollWorker workerFor(Subscription subscription) {
        return worker(subscription.workerKey(),
                subscription.queueName == null
                        ? groupId + ".fanout." + instanceId
                        : groupId + ".queue." + subscription.queueName,
                subscription.queueName != null);
    }

    /** Returns the live worker for {@code key}, creating it and waiting for its consumer. Holds {@code lifecycle}. */
    private PollWorker worker(String key, String consumerGroup, boolean queue) {
        PollWorker existing = workers.get(key);
        if (existing != null) {
            return existing;
        }
        PollWorker w = new PollWorker(key, consumerGroup, queue);
        workers.put(key, w);
        w.thread.start();
        try {
            w.ready.get(CONSUMER_CREATE_TIMEOUT_S, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            workers.remove(key, w);
            throw new IllegalStateException("Could not create the Kafka consumer for " + key + ": "
                    + e.getCause().getMessage(), e.getCause());
        } catch (TimeoutException e) {
            w.requestStop();
            workers.remove(key, w);
            throw new IllegalStateException("Timed out creating the Kafka consumer for " + key, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            w.requestStop();
            workers.remove(key, w);
            throw new IllegalStateException("Interrupted while creating the Kafka consumer for " + key, e);
        }
        return w;
    }

    @Override
    public CompletableFuture<Void> start() {
        synchronized (lifecycle) {
            if (running) {
                return CompletableFuture.completedFuture(null);
            }
            try {
                producer = new KafkaProducer<>(producerProperties());
                executor = Executors.newVirtualThreadPerTaskExecutor();
                worker(FANOUT_KEY, groupId + ".fanout." + instanceId, false);
                for (Subscription s : subscriptions.values()) {
                    workerFor(s).enqueue(new Change(s, true));
                }
                running = true;
                logger.info("KafkaEventBroker started for instance {} ({} subscriptions)",
                        instanceId, subscriptions.size());
                return CompletableFuture.completedFuture(null);
            } catch (RuntimeException e) {
                running = false;
                shutdownLocked();
                logger.error("Failed to start KafkaEventBroker: {}", e.getMessage(), e);
                return CompletableFuture.failedFuture(new RuntimeException("Failed to start Kafka broker", e));
            }
        }
    }

    @Override
    public CompletableFuture<Void> stop() {
        synchronized (lifecycle) {
            if (!running) {
                return CompletableFuture.completedFuture(null);
            }
            running = false;
            shutdownLocked();
            logger.info("KafkaEventBroker stopped for instance {}", instanceId);
        }
        return CompletableFuture.completedFuture(null);
    }

    /** Stops every worker, the executor and the producer. Must hold {@code lifecycle}. Safe on a half-started broker. */
    private void shutdownLocked() {
        List<PollWorker> stopped = new ArrayList<>(workers.values());
        workers.clear();
        for (PollWorker w : stopped) {
            w.requestStop();
        }
        for (PollWorker w : stopped) {
            w.awaitTermination(STOP_JOIN_MS);
        }

        ExecutorService ex = executor;
        executor = null;
        if (ex != null) {
            ex.shutdown();
            try {
                if (!ex.awaitTermination(5, TimeUnit.SECONDS)) {
                    ex.shutdownNow();
                }
            } catch (InterruptedException e) {
                ex.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        KafkaProducer<String, String> p = producer;
        producer = null;
        if (p != null) {
            try {
                p.close(Duration.ofSeconds(5));
            } catch (RuntimeException e) {
                logger.warn("Error closing Kafka producer: {}", e.getMessage());
            }
        }
    }

    /** Test hook: number of live poll workers. */
    int workerCount() {
        return workers.size();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public String getBrokerType() {
        return "kafka";
    }

    /**
     * Test hook: waits until the queue consumer for {@code queueName} on this instance has at least one
     * partition assigned. Returns false if that does not happen within the timeout.
     */
    boolean awaitQueueAssignment(String queueName, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            PollWorker w = workers.get(QUEUE_KEY_PREFIX + queueName);
            if (w != null && w.assignedPartitions > 0) {
                return true;
            }
            Thread.sleep(50);
        }
        return false;
    }

    private Properties producerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.putAll(extraProperties);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    private Properties consumerProperties(String consumerGroup) {
        Properties props = new Properties();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Topics created after a pattern subscription are discovered on the next metadata refresh, which happens
        // at most every metadata.max.age.ms; the group rebalance that follows adds a little more.
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, METADATA_MAX_AGE_MS);
        props.putAll(extraProperties);
        // Offsets are committed by the poll thread after the handlers of a batch finished (at-least-once).
        // Auto-commit would commit on poll, before the asynchronous handlers run, so it is never allowed.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    /** Serializes an Event as a CloudEvents structured JSON document. */
    static String encode(Event event) {
        byte[] bytes = JSON_FORMAT.serialize(event.getCloudEvent());
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /** Parses a CloudEvents structured JSON document into an Event. Throws on malformed input. */
    static Event decode(String json) {
        if (json == null) {
            throw new IllegalArgumentException("null record value");
        }
        CloudEvent ce = JSON_FORMAT.deserialize(json.getBytes(StandardCharsets.UTF_8));
        return new Event(ce);
    }
}
