package com.github.jnthnclt.os.lab.core.stress;

import com.github.jnthnclt.os.lab.collections.bah.LRUConcurrentBAHLinkedHash;
import com.github.jnthnclt.os.lab.core.LABEnvironment;
import com.github.jnthnclt.os.lab.core.LABHeapPressure;
import com.github.jnthnclt.os.lab.core.LABStats;
import com.github.jnthnclt.os.lab.core.api.Keys.KeyStream;
import com.github.jnthnclt.os.lab.core.api.MemoryRawEntryFormat;
import com.github.jnthnclt.os.lab.core.api.ValueIndex;
import com.github.jnthnclt.os.lab.core.api.ValueIndexConfig;
import com.github.jnthnclt.os.lab.core.api.rawhide.LABRawhide;
import com.github.jnthnclt.os.lab.core.guts.LABHashIndexType;
import com.github.jnthnclt.os.lab.core.guts.Leaps;
import com.github.jnthnclt.os.lab.core.guts.StripingBolBufferLocks;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import com.github.jnthnclt.os.lab.core.io.api.UIO;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import java.io.File;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class GlobalHeapStressNGTest {

    @Test(enabled = false)
    public void globalHeapTest() throws Exception {

        LABHashIndexType indexType = LABHashIndexType.cuckoo;
        double hashIndexLoadFactor = 2d;
        File rootA = Files.createTempDir();
        File rootB = Files.createTempDir();
        System.out.println(rootA.getAbsolutePath());
        System.out.println(rootB.getAbsolutePath());
        AtomicLong globalHeapCostInBytes = new AtomicLong();
        LABStats stats = new LABStats(globalHeapCostInBytes);
        ValueIndex indexA = createIndex(rootA, indexType, hashIndexLoadFactor, stats, globalHeapCostInBytes, 10, 10);
        ValueIndex indexB = createIndex(rootB, indexType, hashIndexLoadFactor, stats, globalHeapCostInBytes, 10, 10);


        long totalCardinality = 10_000_000_000L;

        ExecutorService executorService = Executors.newFixedThreadPool(4);

        List<Future<String>> futures  = Lists.newArrayList();
        for (int i = 0; i < 4; i++) {
            ValueIndex index = i % 2 == 0 ? indexA : indexB;
            futures.add(executorService.submit(() -> {
                try {
                    return stress(
                        stats,
                        index,
                        0,
                        totalCardinality,
                        100_000, // writesPerSecond
                        100_000_000, //writeCount
                        1, //readForNSeconds
                        100_000_000, // readCount
                        true,
                        globalHeapCostInBytes); // removes
                } catch (Exception e) {
                    e.printStackTrace();
                    return "oops";
                }
            }));

        }

        for (Future<String> future : futures) {
            System.out.println(future.get());
        }
    }

    private String stress(
        LABStats stats,
        ValueIndex index,
        long offset,
        long totalCardinality,
        int writesPerSecond,
        int writeCount,
        int readForNSeconds,
        int readCount,
        boolean removes,
        AtomicLong globalHeapCostInBytes) throws Exception {

        AtomicLong value = new AtomicLong();
        AtomicLong count = new AtomicLong();

        long totalWriteTime = 0;
        long totalWrites = 0;

        long totalReadTime = 0;
        long totalReads = 0;

        long totalHits = 0;
        long totalMiss = 0;

        Random rand = new Random(12345);

        int c = 0;
        byte[] keyBytes = new byte[8];
        byte[] valuesBytes = new byte[8];
        BolBuffer rawEntryBuffer = new BolBuffer();
        BolBuffer keyBuffer = new BolBuffer();
        while ((writeCount > 0 && totalWrites < writeCount) || (readCount > 0 && totalReads < readCount)) {
            long start = System.currentTimeMillis();
            long writeElapse = 0;
            if (writeCount > 0 && totalWrites < writeCount) {
                long preWriteCount = count.get();
                index.append((stream) -> {
                    for (int i = 0; i < writesPerSecond; i++) {
                        count.incrementAndGet();
                        long key = offset + (long)(rand.nextDouble() * totalCardinality);
                        long timestamp = System.currentTimeMillis();
                        stream.stream(-1,
                            UIO.longBytes(key, keyBytes, 0),
                            timestamp,
                            (removes) ? rand.nextBoolean() : false,
                            timestamp,
                            UIO.longBytes(value.incrementAndGet(), valuesBytes, 0));
                    }
                    return true;
                }, true, rawEntryBuffer, keyBuffer);

                long wrote = count.get() - preWriteCount;
                totalWrites += wrote;
                writeElapse = (System.currentTimeMillis() - start);
                totalWriteTime += writeElapse;
                if (writeElapse < 1000) {
                    Thread.sleep(1000 - writeElapse);
                }
            }

            start = System.currentTimeMillis();
            long readElapse = 0;
            AtomicLong misses = new AtomicLong();
            AtomicLong hits = new AtomicLong();
            if (readCount > 0 && totalReads < readCount) {
                long s = start;

                while (System.currentTimeMillis() - s < (1000 * readForNSeconds)) {

                    index.get((KeyStream keyStream) -> {
                        long k = (long)(rand.nextDouble() * totalCardinality);
                        UIO.longBytes(k, keyBytes, 0);
                        keyStream.key(0, keyBytes, 0, keyBytes.length);
                        return true;
                    }, (index1, key, timestamp, tombstoned, version1, value1) -> {
                        if (value1 != null && !tombstoned) {
                            hits.incrementAndGet();
                        } else {
                            misses.incrementAndGet();
                        }
                        return true;
                    }, true);
                }
                totalReads += misses.get() + hits.get();
                readElapse = (System.currentTimeMillis() - start);
                totalReadTime += readElapse;
                totalHits += hits.get();
                totalMiss += misses.get();
            }

            c++;
            System.out.println("heap:"+globalHeapCostInBytes.get()+ " debt:"+index.debt()+" gcCommits:"+stats.gcCommit.sum());
        }

        double totalReadRate = totalReadTime > 0 ? (double) totalReads * 1000 / (totalReadTime) : 0;
        double totalWriteRate = totalWriteTime > 0 ? (double) totalWrites * 1000 / (totalWriteTime) : 0;

        AtomicLong scanCount = new AtomicLong();
        index.rowScan((index1, key, timestamp, tombstoned, version1, payload) -> {
            if (!tombstoned) {
                scanCount.incrementAndGet();
            }
            return true;
        }, true);

        String punchLine = "index:" + scanCount.get()
            + " writeMillis:" + totalWriteTime
            + " write:" + totalWrites
            + " wps:" + totalWriteRate
            + " readMillis:" + totalReadTime
            + " read:" + totalReads
            + " rps:" + totalReadRate
            + " hits:" + totalHits
            + " miss:" + totalMiss;

        return punchLine;
    }
    

    private ValueIndex createIndex(File root,
        LABHashIndexType indexType,
        double hashIndexLoadFactor,
        LABStats stats,
        AtomicLong globalHeapCostInBytes,
        int maxHeapPreasureMB,
        int blockOnHeapPreasureMB) throws Exception {

        System.out.println("Created root " + root);
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100_000, 8);
        LABHeapPressure labHeapPressure = new LABHeapPressure(stats,
            LABEnvironment.buildLABHeapSchedulerThreadPool(1),
            "default",
            1024 * 1024 * maxHeapPreasureMB,
            1024 * 1024 * blockOnHeapPreasureMB,
            globalHeapCostInBytes,
            LABHeapPressure.FreeHeapStrategy.mostBytesFirst);

        LABEnvironment env = new LABEnvironment(stats,
            LABEnvironment.buildLABSchedulerThreadPool(1),
            LABEnvironment.buildLABCompactorThreadPool(Math.max(1,Runtime.getRuntime().availableProcessors()-1)), // compact
            LABEnvironment.buildLABDestroyThreadPool(1), // destroy
            null,
            root, // rootFile
            labHeapPressure,
            4, // minMergeDebt
            8, // maxMergeDebt
            leapsCache,
            new StripingBolBufferLocks(1024),
            true,
            false);


        System.out.println("Created env");
        ValueIndex index = env.open(new ValueIndexConfig("foo",
            1024 * 4, // entriesBetweenLeaps
            1024 * 1024 * maxHeapPreasureMB, // maxHeapPressureInBytes
            -1, // splitWhenKeysTotalExceedsNBytes
            -1, // splitWhenValuesTotalExceedsNBytes
            1024 * 1024 * 64, // splitWhenValuesAndKeysTotalExceedsNBytes
            "deprecated",
            LABRawhide.NAME,
            MemoryRawEntryFormat.NAME,
            24,
            indexType,
            hashIndexLoadFactor,
            true,
            1000));
        return index;
    }



}
