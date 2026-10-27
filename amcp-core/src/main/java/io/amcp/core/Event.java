package io.amcp.core;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.UUID;

/**
 * Wrapper for CloudEvents that provides convenient access to AMCP event data.
 * This class bridges AMCP's internal event model with the CloudEvents standard.
 */
public class Event {
    private final CloudEvent cloudEvent;
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    /**
     * Creates an Event from a CloudEvent.
     *
     * @param cloudEvent the CloudEvent
     */
    public Event(CloudEvent cloudEvent) {
        this.cloudEvent = Objects.requireNonNull(cloudEvent, "CloudEvent cannot be null");
    }

    /**
     * Creates a new Event with the specified topic and payload.
     *
     * @param topic   the event topic (e.g., "hello.request")
     * @param payload the event payload
     * @return a new Event
     */
    public static Event create(String topic, Object payload) {
        return create(topic, "https://amcp.dev/system", payload);
    }

    /**
     * Creates a new Event with the specified topic, source, and payload.
     *
     * @param topic   the event topic
     * @param source  the event source URI
     * @param payload the event payload
     * @return a new Event
     */
    public static Event create(String topic, String source, Object payload) {
        try {
            byte[] data = MAPPER.writeValueAsBytes(payload);
            CloudEvent ce = CloudEventBuilder.v1()
                    .withId(UUID.randomUUID().toString())
                    .withType("io.amcp.event." + topic)
                    .withSource(URI.create(source))
                    .withTime(OffsetDateTime.now())
                    .withDataContentType("application/json")
                    .withData(data)
                    .build();
            return new Event(ce);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create event", e);
        }
    }

    /**
     * Gets the event ID.
     *
     * @return the event ID
     */
    public String getId() {
        return cloudEvent.getId();
    }

    /**
     * Gets the event topic (extracted from CloudEvents type).
     * Converts "io.amcp.event.hello.request" to "hello.request".
     *
     * @return the event topic
     */
    public String getTopic() {
        String type = cloudEvent.getType();
        if (type.startsWith("io.amcp.event.")) {
            return type.substring("io.amcp.event.".length());
        }
        return type;
    }

    /**
     * Gets the full CloudEvents type.
     *
     * @return the event type
     */
    public String getType() {
        return cloudEvent.getType();
    }

    /**
     * Gets the event source.
     *
     * @return the event source URI
     */
    public URI getSource() {
        return cloudEvent.getSource();
    }

    /**
     * Gets the event timestamp.
     *
     * @return the event time
     */
    public OffsetDateTime getTime() {
        return cloudEvent.getTime();
    }

    /**
     * Gets the event payload as the specified type.
     *
     * @param <T>  the payload type
     * @param type the class of the payload type
     * @return the deserialized payload
     */
    public <T> T getPayload(Class<T> type) {
        try {
            byte[] data = cloudEvent.getData() != null ? cloudEvent.getData().toBytes() : new byte[0];
            return MAPPER.readValue(data, type);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize payload", e);
        }
    }

    /**
     * Gets the raw payload data.
     *
     * @return the payload bytes, or null if no data
     */
    public byte[] getData() {
        return cloudEvent.getData() != null ? cloudEvent.getData().toBytes() : null;
    }

    /**
     * Gets the underlying CloudEvent.
     *
     * @return the CloudEvent
     */
    public CloudEvent getCloudEvent() {
        return cloudEvent;
    }

    /**
     * Checks if the topic matches the specified pattern.
     * Supports wildcards: "hello.*" matches "hello.request", "hello.response", etc.
     *
     * @param pattern the topic pattern
     * @return true if the topic matches
     */
    public boolean matchesTopic(String pattern) {
        String topic = getTopic();
        
        if (pattern.equals(topic)) {
            return true;
        }
        
        // Handle wildcards. A wildcard never matches an empty segment, so topics with a trailing dot
        // ("p.x.") or a doubled dot ("p..x") match only by exact equality. This is the same rule as
        // io.amcp.broker.kafka.TopicPatterns.toRegex.
        if (pattern.endsWith(".**")) {
            // "p.**" matches "p" itself and "p.<one or more non-empty segments>", never "px".
            String prefix = pattern.substring(0, pattern.length() - 3);
            if (topic.equals(prefix)) {
                return true;
            }
            return topic.startsWith(prefix + ".") && nonEmptySegments(topic.substring(prefix.length() + 1));
        }

        if (pattern.endsWith(".*")) {
            // "p.*" matches "p.<exactly one non-empty segment>".
            String prefix = pattern.substring(0, pattern.length() - 2);
            if (!topic.startsWith(prefix + ".")) {
                return false;
            }
            String rest = topic.substring(prefix.length() + 1);
            return !rest.isEmpty() && rest.indexOf('.') < 0;
        }

        return false;
    }

    /** True when {@code rest} is one or more dot-separated segments and none of them is empty. */
    private static boolean nonEmptySegments(String rest) {
        return !rest.isEmpty() && !rest.startsWith(".") && !rest.endsWith(".") && !rest.contains("..");
    }

    @Override
    public String toString() {
        return String.format("Event{id=%s, topic=%s, source=%s, time=%s}",
                getId(), getTopic(), getSource(), getTime());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return Objects.equals(cloudEvent.getId(), event.cloudEvent.getId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(cloudEvent.getId());
    }
}
