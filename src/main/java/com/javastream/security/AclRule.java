package com.javastream.security;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/** Single principal → topic pattern → permissions rule. */
public record AclRule(String principal, String topicPattern, Set<TopicPermission> permissions) {

    public AclRule {
        Objects.requireNonNull(principal, "principal");
        Objects.requireNonNull(topicPattern, "topicPattern");
        permissions = permissions == null || permissions.isEmpty()
                ? EnumSet.noneOf(TopicPermission.class)
                : EnumSet.copyOf(permissions);
    }

    public boolean matchesPrincipal(String user) {
        return principal.equals("*") || principal.equals(user);
    }

    public boolean matchesTopic(String topic) {
        if (topicPattern.equals("*")) {
            return true;
        }
        if (topicPattern.endsWith("*")) {
            String prefix = topicPattern.substring(0, topicPattern.length() - 1);
            return topic.startsWith(prefix);
        }
        return topicPattern.equals(topic);
    }

    public boolean grants(TopicPermission permission) {
        return permissions.contains(permission);
    }
}
