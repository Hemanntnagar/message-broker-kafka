package com.javastream.security;

/** Topic-level access for producers and consumers. */
public enum TopicPermission {
    PRODUCE,
    CONSUME,
    ADMIN;

    public static TopicPermission parse(String token) {
        return switch (token.trim().toUpperCase()) {
            case "PRODUCE", "WRITE", "PUBLISH" -> PRODUCE;
            case "CONSUME", "READ", "FETCH" -> CONSUME;
            case "ADMIN", "ALL", "*" -> ADMIN;
            default -> throw new IllegalArgumentException("Unknown permission: " + token);
        };
    }
}
