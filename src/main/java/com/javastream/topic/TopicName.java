package com.javastream.topic;

import com.javastream.common.Preconditions;

/**
 * Value object for a topic identifier.
 */
public final class TopicName {

    private final String value;

    public TopicName(String value) {
        this.value = Preconditions.requireNonBlank(value, "topic name");
    }

    public String value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TopicName other)) {
            return false;
        }
        return value.equals(other.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return value;
    }
}
