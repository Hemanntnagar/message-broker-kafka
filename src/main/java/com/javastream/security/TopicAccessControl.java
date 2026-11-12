package com.javastream.security;

import java.util.ArrayList;
import java.util.List;

/** Topic-level authorization (producer / consumer / admin). */
public final class TopicAccessControl {

    private final boolean enabled;
    private final List<AclRule> rules;

    private TopicAccessControl(boolean enabled, List<AclRule> rules) {
        this.enabled = enabled;
        this.rules = List.copyOf(rules);
    }

    public static TopicAccessControl disabled() {
        return new TopicAccessControl(false, List.of());
    }

    public static TopicAccessControl of(boolean enabled, List<AclRule> rules) {
        return new TopicAccessControl(enabled, rules == null ? List.of() : new ArrayList<>(rules));
    }

    public boolean enabled() {
        return enabled;
    }

    public boolean isAllowed(String principal, String topic, TopicPermission permission) {
        if (!enabled) {
            return true;
        }
        String user = principal == null || principal.isBlank() ? "anonymous" : principal;
        for (AclRule rule : rules) {
            if (rule.matchesPrincipal(user) && rule.matchesTopic(topic) && rule.grants(permission)) {
                return true;
            }
        }
        return false;
    }
}
