package com.javastream.benchmark;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;

/** JVM and OS resource snapshot during a benchmark run. */
public record ResourceSampler(
        double processCpuLoad,
        long heapUsedBytes,
        long heapMaxBytes,
        long gcCollectionCount,
        long gcCollectionTimeMillis) {

    public static ResourceSampler capture() {
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        double cpu = -1.0;
        if (os instanceof com.sun.management.OperatingSystemMXBean sunOs) {
            cpu = sunOs.getProcessCpuLoad();
        }
        MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        long heapUsed = memory.getHeapMemoryUsage().getUsed();
        long heapMax = memory.getHeapMemoryUsage().getMax();
        long gcCount = 0;
        long gcTime = 0;
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            gcCount += gc.getCollectionCount();
            gcTime += gc.getCollectionTime();
        }
        return new ResourceSampler(cpu, heapUsed, heapMax, gcCount, gcTime);
    }

    public double heapUsedMb() {
        return heapUsedBytes / (1024.0 * 1024.0);
    }
}
