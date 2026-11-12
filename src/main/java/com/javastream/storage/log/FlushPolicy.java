package com.javastream.storage.log;

/**
 * Controls when appended log bytes are forced to stable storage.
 *
 * <p><strong>NONE</strong> — Records are written through the OS page cache only. A process crash may
 * lose the tail of the log that was not yet flushed by the kernel. {@link java.nio.channels.FileChannel#force}
 * is not called on append. Lowest latency; weakest durability.
 *
 * <p><strong>PERIODIC</strong> — Appends buffer in memory; a background thread calls {@code force(false)} on
 * active segment files at {@code storage.flush.interval.ms}. A crash may lose records since the last
 * periodic flush (bounded by the interval). Explicit {@link com.javastream.storage.BrokerMessageStore#flush()}
 * still forces immediately.
 *
 * <p><strong>SYNC</strong> — After each successful append, {@code force(true)} is invoked on the segment
 * file (metadata and data). Highest durability per record; highest latency. Survives process crash as long as
 * the filesystem honors fsync.
 */
public enum FlushPolicy {
    NONE,
    PERIODIC,
    SYNC
}
