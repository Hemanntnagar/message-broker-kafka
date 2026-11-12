package com.javastream.delivery;

import java.time.Duration;
import java.util.Objects;

/**
 * Client-side producer reliability settings.
 */
public record ProducerDeliverySettings(
        ProducerAckLevel ackLevel,
        Duration requestTimeout,
        int maxRetries,
        Duration retryBackoff,
        boolean idempotent,
        long producerId) {

    public static final ProducerDeliverySettings DEFAULTS =
            new ProducerDeliverySettings(
                    ProducerAckLevel.ACK_LEADER,
                    Duration.ofSeconds(30),
                    3,
                    Duration.ofMillis(100),
                    false,
                    -1);

    public ProducerDeliverySettings {
        Objects.requireNonNull(ackLevel, "ackLevel");
        Objects.requireNonNull(requestTimeout, "requestTimeout");
        Objects.requireNonNull(retryBackoff, "retryBackoff");
        if (requestTimeout.isNegative() || requestTimeout.isZero()) {
            throw new IllegalArgumentException("requestTimeout must be positive");
        }
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be non-negative");
        }
        if (retryBackoff.isNegative()) {
            throw new IllegalArgumentException("retryBackoff must be non-negative");
        }
        if (idempotent && producerId < 0) {
            throw new IllegalArgumentException("idempotent producers require producerId >= 0");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private ProducerAckLevel ackLevel = DEFAULTS.ackLevel();
        private Duration requestTimeout = DEFAULTS.requestTimeout();
        private int maxRetries = DEFAULTS.maxRetries();
        private Duration retryBackoff = DEFAULTS.retryBackoff();
        private boolean idempotent = DEFAULTS.idempotent();
        private long producerId = DEFAULTS.producerId();

        public Builder ackLevel(ProducerAckLevel ackLevel) {
            this.ackLevel = ackLevel;
            return this;
        }

        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder retryBackoff(Duration retryBackoff) {
            this.retryBackoff = retryBackoff;
            return this;
        }

        public Builder idempotent(long producerId) {
            this.idempotent = true;
            this.producerId = producerId;
            return this;
        }

        public ProducerDeliverySettings build() {
            return new ProducerDeliverySettings(
                    ackLevel, requestTimeout, maxRetries, retryBackoff, idempotent, producerId);
        }
    }
}
