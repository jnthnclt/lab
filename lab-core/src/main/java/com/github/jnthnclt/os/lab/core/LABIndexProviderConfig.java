package com.github.jnthnclt.os.lab.core;

import com.github.jnthnclt.os.lab.core.api.ValueIndexConfig;
import com.github.jnthnclt.os.lab.core.guts.LABFiles;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class LABIndexProviderConfig {
    private final int cores = Runtime.getRuntime().availableProcessors();
    private  ExecutorService scheduler = LABEnvironment.buildLABSchedulerThreadPool(cores);
    private  ExecutorService compact = LABEnvironment.buildLABCompactorThreadPool(cores);
    private  ExecutorService destroy = LABEnvironment.buildLABDestroyThreadPool(cores);
    private  ExecutorService heapScheduler = LABEnvironment.buildLABHeapSchedulerThreadPool(cores);

    private  LABStats stats = null;
    private  LABFiles labFiles =null;
    private long maxHeapPressureInBytes = 1024 * 1024 * 512;
    private long blockOnHeapPressureInBytes = 1024 * 1024 * 768;
    private AtomicLong globalHeapCostInBytes = new AtomicLong();
    private LABHeapPressure.FreeHeapStrategy freeHeapStrategy = LABHeapPressure.FreeHeapStrategy.mostBytesFirst;

    private LABWALConfig walConfig;
    private int minMergeDebt;
    private int maxMergeDebt;

    private ValueIndexConfig valueIndexConfig;
}
