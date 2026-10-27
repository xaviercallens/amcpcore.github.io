package io.amcp.broker.kafka;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for the AMCP-pattern to Kafka-regex translation. No Kafka needed. */
class TopicPatternsTest {

    private static boolean matches(String amcpPattern, String topic) {
        return Pattern.compile(TopicPatterns.toRegex(amcpPattern)).matcher(topic).matches();
    }

    @Test
    void exactPatternMatchesOnlyThatTopicAndTreatsDotsLiterally() {
        assertTrue(matches("a.b.c", "a.b.c"));
        assertFalse(matches("a.b.c", "a.b.cd"));
        assertFalse(matches("a.b.c", "aXb.c"), "dots are literal, not regex wildcards");
        assertFalse(matches("a.b.c", "a.b.c.d"));
    }

    @Test
    void singleStarMatchesExactlyOneSegment() {
        assertTrue(matches("p.*", "p.x"));
        assertFalse(matches("p.*", "p"));
        assertFalse(matches("p.*", "p.x.y"));
        assertFalse(matches("p.*", "px.x"));
        assertFalse(matches("p.*", "p."), "an empty segment is not a match");
    }

    @Test
    void doubleStarMatchesPrefixAndAnyDepthButRespectsSegmentBoundary() {
        assertTrue(matches("p.**", "p"));
        assertTrue(matches("p.**", "p.x"));
        assertTrue(matches("p.**", "p.x.y.z"));
        assertFalse(matches("p.**", "px"), "p.** must not match a longer first segment");
        assertFalse(matches("p.**", "phive.x"));
        assertFalse(matches("p.**", "x.p"));
    }

    @Test
    void unionMatchesAnyMemberAndNothingElse() {
        Pattern union = TopicPatterns.union(List.of("a.b", "c.*", "d.**"));
        assertTrue(union.matcher("a.b").matches());
        assertTrue(union.matcher("c.x").matches());
        assertTrue(union.matcher("d").matches());
        assertTrue(union.matcher("d.e.f").matches());
        assertFalse(union.matcher("c").matches());
        assertFalse(union.matcher("a.c").matches());
        assertFalse(union.matcher("dx").matches());
    }

    @Test
    void unionOfNothingIsNull() {
        assertNull(TopicPatterns.union(Set.of()));
    }

    @Test
    void unionAcceptsSortedSetOfPatterns() {
        Set<String> wanted = new TreeSet<>(List.of("z.*", "a.**"));
        Pattern union = TopicPatterns.union(wanted);
        assertTrue(union.matcher("a.q").matches());
        assertTrue(union.matcher("z.q").matches());
    }

    /** Pattern, topic, expected. The SAME table runs through Event.matchesTopic and TopicPatterns.toRegex. */
    private static final String[][] AGREEMENT_TABLE = {
            {"a.b.c", "a.b.c", "true"},
            {"a.b.c", "a.b.cd", "false"},
            {"p.*", "p.x", "true"},
            {"p.*", "p", "false"},
            {"p.*", "p.x.y", "false"},
            {"p.*", "px.x", "false"},
            {"p.*", "p.", "false"},
            {"p.*", "p.x.", "false"},
            {"p.*", "p..", "false"},
            {"a.b.*", "a.b.x", "true"},
            {"a.b.*", "a.b.x.", "false"},
            {"p.**", "p", "true"},
            {"p.**", "p.x", "true"},
            {"p.**", "p.x.y", "true"},
            {"p.**", "px", "false"},
            {"p.**", "p.", "false"},
            {"p.**", "p.x.", "false"},
            {"p.**", "p..x", "false"},
            {"p.**", "p.x..y", "false"},
            {"p.", "p.", "true"},
            {"p.", "p", "false"},
            {"p.x.", "p.x.", "true"},
    };

    @Test
    void eventMatchesTopicAndTopicPatternsAgreeOnEveryCase() {
        for (String[] row : AGREEMENT_TABLE) {
            boolean expected = Boolean.parseBoolean(row[2]);
            boolean viaEvent = io.amcp.core.Event.create(row[1], java.util.Map.of("n", 1)).matchesTopic(row[0]);
            boolean viaRegex = matches(row[0], row[1]);
            org.junit.jupiter.api.Assertions.assertEquals(expected, viaEvent,
                    "Event.matchesTopic(" + row[0] + ") on topic '" + row[1] + "'");
            org.junit.jupiter.api.Assertions.assertEquals(expected, viaRegex,
                    "TopicPatterns.toRegex(" + row[0] + ") on topic '" + row[1] + "'");
        }
    }
}
