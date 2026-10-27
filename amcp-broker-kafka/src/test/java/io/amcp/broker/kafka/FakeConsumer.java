package io.amcp.broker.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * A deterministic in-memory consumer for unit tests. It behaves like a partitioned log: records stay in the log, a
 * seek replays them, and every commit is recorded. subscribe(Pattern) assigns every known partition without checking
 * the pattern, which is how a group member can own a partition for a topic it never subscribed to.
 */
class FakeConsumer extends MockConsumer<String, String> {
    final List<ConsumerRecord<String, String>> log = Collections.synchronizedList(new ArrayList<>());
    final List<Map<TopicPartition, OffsetAndMetadata>> commits = Collections.synchronizedList(new ArrayList<>());
    final AtomicInteger subscribeAttempts = new AtomicInteger();
    volatile int failSubscribeTimes = 0;
    private final Map<String, Long> nextOffset = new HashMap<>();
    private final Set<TopicPartition> known = new HashSet<>();
    private boolean subscribed = false;

    FakeConsumer() {
        super(OffsetResetStrategy.EARLIEST);
    }

    synchronized void append(String topic, String value) {
        long offset = nextOffset.merge(topic, 1L, Long::sum) - 1;
        ConsumerRecord<String, String> r = new ConsumerRecord<>(topic, 0, offset, "k", value);
        log.add(r);
        known.add(new TopicPartition(topic, 0));
        if (subscribed) {
            assignKnown();
            addRecord(r);
        }
    }

    private void assignKnown() {
        assign(new HashSet<>(known));
        Map<TopicPartition, Long> begin = new HashMap<>();
        for (TopicPartition tp : known) {
            begin.put(tp, 0L);
        }
        updateBeginningOffsets(begin);
    }

    @Override
    public synchronized void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        subscribeAttempts.incrementAndGet();
        if (failSubscribeTimes > 0) {
            failSubscribeTimes--;
            throw new IllegalStateException("simulated subscribe failure");
        }
        if (!subscribed) {
            subscribed = true;
            assignKnown();
            for (ConsumerRecord<String, String> r : new ArrayList<>(log)) {
                addRecord(r);
            }
        }
    }

    @Override
    public synchronized void subscribe(Pattern pattern) {
        subscribe(pattern, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(java.util.Collection<TopicPartition> partitions) {
            }

            @Override
            public void onPartitionsAssigned(java.util.Collection<TopicPartition> partitions) {
            }
        });
    }

    @Override
    public synchronized ConsumerRecords<String, String> poll(Duration timeout) {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return super.poll(timeout);
    }

    @Override
    public synchronized void seek(TopicPartition partition, long offset) {
        super.seek(partition, offset);
        for (ConsumerRecord<String, String> r : new ArrayList<>(log)) {
            if (r.topic().equals(partition.topic()) && r.offset() >= offset) {
                addRecord(r);
            }
        }
    }

    @Override
    public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        commits.add(new HashMap<>(offsets));
        super.commitSync(offsets);
    }
}
