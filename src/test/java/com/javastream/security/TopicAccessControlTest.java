package com.javastream.security;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicAccessControlTest {

    @Test
    void disabledAclAllowsAll() {
        TopicAccessControl acl = TopicAccessControl.disabled();
        assertTrue(acl.isAllowed("alice", "orders", TopicPermission.PRODUCE));
    }

    @Test
    void enforcedRulesMatchTopicPatterns() {
        TopicAccessControl acl =
                TopicAccessControl.of(
                        true,
                        List.of(
                                new AclRule("alice", "orders", Set.of(TopicPermission.PRODUCE)),
                                new AclRule("bob", "logs*", Set.of(TopicPermission.CONSUME))));
        assertTrue(acl.isAllowed("alice", "orders", TopicPermission.PRODUCE));
        assertFalse(acl.isAllowed("alice", "orders", TopicPermission.CONSUME));
        assertTrue(acl.isAllowed("bob", "logs-east", TopicPermission.CONSUME));
        assertFalse(acl.isAllowed("bob", "orders", TopicPermission.CONSUME));
    }
}
