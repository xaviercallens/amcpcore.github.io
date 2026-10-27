package io.amcp.broker.kafka;

import java.util.Collection;
import java.util.StringJoiner;
import java.util.regex.Pattern;

/**
 * Translates AMCP topic patterns into Kafka topic regexes.
 *
 * <ul>
 *   <li>{@code a.b.c} (no wildcard) matches exactly the topic {@code a.b.c}.</li>
 *   <li>{@code p.*} matches {@code p.X} where X is exactly one non-empty segment.</li>
 *   <li>{@code p.**} matches {@code p} itself and {@code p.<one or more non-empty segments>}, never {@code px}.</li>
 * </ul>
 *
 * <p>A wildcard never matches an empty segment: {@code p.x.} and {@code p..x} match {@code p.*} / {@code p.**} not at
 * all (they match only a pattern equal to the topic). {@code io.amcp.core.Event#matchesTopic} follows the same rule.
 *
 * <p>Kafka matches a subscription pattern against the whole topic name, so the regexes produced
 * here are anchored by the caller's {@code matches()} call.
 */
final class TopicPatterns {

    private TopicPatterns() {
    }

    /**
     * Returns the regex (not compiled) for one AMCP topic pattern.
     */
    static String toRegex(String amcpPattern) {
        if (amcpPattern.endsWith(".**")) {
            String prefix = amcpPattern.substring(0, amcpPattern.length() - 3);
            return Pattern.quote(prefix) + "(\\.[^.]+(\\.[^.]+)*)?";
        }
        if (amcpPattern.endsWith(".*")) {
            String prefix = amcpPattern.substring(0, amcpPattern.length() - 2);
            return Pattern.quote(prefix) + "\\.[^.]+";
        }
        return Pattern.quote(amcpPattern);
    }

    /**
     * Returns one compiled pattern that matches any topic matched by any of the given AMCP patterns,
     * or {@code null} when the collection is empty.
     */
    static Pattern union(Collection<String> amcpPatterns) {
        if (amcpPatterns.isEmpty()) {
            return null;
        }
        StringJoiner joiner = new StringJoiner("|", "(?:", ")");
        for (String p : amcpPatterns) {
            joiner.add(toRegex(p));
        }
        return Pattern.compile(joiner.toString());
    }
}
