package com.javastream.topic;

import com.javastream.exception.JavaStreamException;

import java.util.regex.Pattern;

/** Validates topic name syntax before registration. */
public final class TopicNameValidator {

    private static final int MAX_LENGTH = 249;
    private static final Pattern ALLOWED = Pattern.compile("^[a-zA-Z0-9._-]+$");

    private TopicNameValidator() {}

    public static String validate(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new JavaStreamException("topic name must be non-blank");
        }
        String trimmed = raw.trim();
        if (trimmed.length() > MAX_LENGTH) {
            throw new JavaStreamException("topic name must be at most " + MAX_LENGTH + " characters");
        }
        if (!ALLOWED.matcher(trimmed).matches()) {
            throw new JavaStreamException(
                    "topic name must contain only letters, digits, dots, underscores, or hyphens");
        }
        return trimmed;
    }
}
