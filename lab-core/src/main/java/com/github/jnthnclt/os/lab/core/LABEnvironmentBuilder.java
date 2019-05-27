package com.github.jnthnclt.os.lab.core;

import com.github.jnthnclt.os.lab.collections.bah.LRUConcurrentBAHLinkedHash;
import com.github.jnthnclt.os.lab.core.guts.LABFiles;
import com.github.jnthnclt.os.lab.core.guts.Leaps;
import com.github.jnthnclt.os.lab.core.guts.StripingBolBufferLocks;

import java.io.File;
import java.util.concurrent.ExecutorService;

public class LABEnvironmentBuilder {

    private final int cores = Runtime.getRuntime().availableProcessors();

    public LABFiles labFiles = null;
    public ExecutorService scheduler = LABEnvironment.buildLABSchedulerThreadPool(cores);
    public ExecutorService compact = LABEnvironment.buildLABCompactorThreadPool(cores);
    public ExecutorService destroy = LABEnvironment.buildLABDestroyThreadPool(cores);

    public int minMergeDebt = 4;
    public int maxMergeDebt = 16;
    public LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100_000, 4);
    public StripingBolBufferLocks stripingBolBufferLocks = new StripingBolBufferLocks(1024);

    public LABEnvironment build(LABStats stats,
                                File root,
                                LABHeapPressure labHeapPressure) throws Exception {
        return new LABEnvironment(stats,
                labFiles,
                scheduler,
                compact,
                destroy,
                null,
                root,
                labHeapPressure,
                minMergeDebt,
                maxMergeDebt,
                leapsCache,
                stripingBolBufferLocks,
                true,
                true);
    }

    public LABEnvironmentBuilder setLABFiles(LABFiles labFiles) {
        this.labFiles = labFiles;
        return this;
    }

    public LABEnvironmentBuilder setMinMergeDebt(int minMergeDebt) {
        this.minMergeDebt = minMergeDebt;
        return this;
    }

    public LABEnvironmentBuilder setMaxMergeDebt(int maxMergeDebt) {
        this.maxMergeDebt = maxMergeDebt;
        return this;
    }
}
