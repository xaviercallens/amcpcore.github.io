package io.amcp.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EventTest {

    @Test
    void testCreateEvent() {
        Event event = Event.create("test.topic", "testPayload");
        
        assertNotNull(event.getId());
        assertEquals("test.topic", event.getTopic());
        assertEquals("io.amcp.event.test.topic", event.getType());
        assertEquals("testPayload", event.getPayload(String.class));
    }

    @Test
    void testEventWithCustomSource() {
        Event event = Event.create("test.topic", "https://example.com", "payload");
        
        assertEquals("https://example.com", event.getSource().toString());
    }

    @Test
    void testTopicMatching() {
        Event event = Event.create("hello.request", "test");
        
        assertTrue(event.matchesTopic("hello.request"));
        assertTrue(event.matchesTopic("hello.*"));
        assertTrue(event.matchesTopic("hello.**"));
        assertFalse(event.matchesTopic("goodbye.*"));
    }

    @Test
    void testWildcardMatching() {
        Event event1 = Event.create("hello.request", "test");
        Event event2 = Event.create("hello.response.success", "test");
        
        // Single level wildcard
        assertTrue(event1.matchesTopic("hello.*"));
        assertFalse(event2.matchesTopic("hello.*"));
        
        // Multi-level wildcard
        assertTrue(event1.matchesTopic("hello.**"));
        assertTrue(event2.matchesTopic("hello.**"));
    }

    @Test
    void testDoubleStarRespectsSegmentBoundary() {
        assertTrue(Event.create("p", "test").matchesTopic("p.**"), "p.** matches the bare prefix p");
        assertTrue(Event.create("p.x", "test").matchesTopic("p.**"));
        assertTrue(Event.create("p.x.y", "test").matchesTopic("p.**"));
        assertFalse(Event.create("px", "test").matchesTopic("p.**"), "p.** must not match phive-style prefixes");
        assertFalse(Event.create("phive.x", "test").matchesTopic("p.**"));
        assertFalse(Event.create("other.p", "test").matchesTopic("p.**"));
    }

    @Test
    void testComplexPayload() {
        record TestData(String name, int value) {}
        
        TestData original = new TestData("test", 42);
        Event event = Event.create("test.data", original);
        
        TestData retrieved = event.getPayload(TestData.class);
        assertEquals("test", retrieved.name());
        assertEquals(42, retrieved.value());
    }
}
