package com.javastream.storage.log;

import java.util.concurrent.atomic.AtomicLong;

/** Partition-local monotonic offset assignment. */
public final class OffsetManager {

    private final AtomicLong nextOffset = new AtomicLong();

    public long assignAndIncrement() {
        return nextOffset.getAndIncrement();
    }

    public void undoLastAssign() {
        nextOffset.updateAndGet(v -> v > 0 ? v - 1 : 0);
    }

    public long peekNext() {
        return nextOffset.get();
    }

    public long logEndOffset() {
        return nextOffset.get();
    }

    public void resetFromRecovery(long recoveredNextOffset, long recoveredRecordCount) {
        if (recoveredNextOffset < 0) {
            throw new IllegalArgumentException("recoveredNextOffset must be non-negative");
        }
        nextOffset.set(recoveredNextOffset);
    }

    public long recordCount() {
        return nextOffset.get();
    }
}
