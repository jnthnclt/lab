package com.github.jnthnclt.os.lab.core;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class LABHeapPressureBuilder {

    public ExecutorService heapScheduler = LABEnvironment.buildLABHeapSchedulerThreadPool(
            Runtime.getRuntime().availableProcessors());
    public String name = "default";
    public long maxHeapPressureInBytes = 1024 * 1024 * 512;
    public long blockOnHeapPressureInBytes = 1024 * 1024 * 768;
    public LABHeapPressure.FreeHeapStrategy freeHeapStrategy = LABHeapPressure.FreeHeapStrategy.mostBytesFirst;
    private final AtomicLong globalHeapCostInBytes;

    public LABHeapPressureBuilder(AtomicLong globalHeapCostInBytes) {
        this.globalHeapCostInBytes = globalHeapCostInBytes;
    }

    public LABHeapPressure build(LABStats stats) {
        return new LABHeapPressure(stats,
                heapScheduler,
                name,
                maxHeapPressureInBytes,
                blockOnHeapPressureInBytes,
                globalHeapCostInBytes,
                freeHeapStrategy
        );
    }

    public LABHeapPressureBuilder setMaxHeapPressureInBytes(long maxHeapPressureInBytes) {
        this.maxHeapPressureInBytes = maxHeapPressureInBytes;
        this.blockOnHeapPressureInBytes = (maxHeapPressureInBytes / 2) * 3;
        return this;
    }
}
