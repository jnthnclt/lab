package com.github.jnthnclt.os.lab.core.stress;

import com.github.jnthnclt.os.lab.base.BolBuffer;
import com.github.jnthnclt.os.lab.base.UIO;
import com.github.jnthnclt.os.lab.collections.bah.LRUConcurrentBAHLinkedHash;
import com.github.jnthnclt.os.lab.core.LAB;
import com.github.jnthnclt.os.lab.core.LABEnvironment;
import com.github.jnthnclt.os.lab.core.LABHeapPressure;
import com.github.jnthnclt.os.lab.core.LABStats;
import com.github.jnthnclt.os.lab.api.Keys.KeyStream;
import com.github.jnthnclt.os.lab.core.api.MemoryRawEntryFormat;
import com.github.jnthnclt.os.lab.api.ValueIndex;
import com.github.jnthnclt.os.lab.core.api.ValueIndexConfig;
import com.github.jnthnclt.os.lab.core.api.rawhide.LABFixedWidthKeyFixedWidthValueRawhide;
import com.github.jnthnclt.os.lab.core.guts.LABFiles;
import com.github.jnthnclt.os.lab.core.guts.LABHashIndexType;
import com.github.jnthnclt.os.lab.core.guts.Leaps;
import com.github.jnthnclt.os.lab.core.guts.StripingBolBufferLocks;
import com.github.jnthnclt.os.lab.log.LABLogger;
import com.github.jnthnclt.os.lab.log.LABLoggerFactory;
import com.google.common.io.Files;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
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
public class LABStress {

    private static final LABLogger LOG = LABLoggerFactory.getLogger();


    @Test(enabled = false)
    public void stressWritesTest() throws Exception {

        boolean enableHashIndex = true;
        //LABHashIndexType indexType = LABHashIndexType.cuckoo;
        //LABHashIndexType indexType = LABHashIndexType.fibCuckoo;
        LABHashIndexType indexType = LABHashIndexType.linearProbe;


        double hashIndexLoadFactor = 2d;
        File root = Files.createTempDir();
        System.out.println(root.getAbsolutePath());
        AtomicLong globalHeapCostInBytes = new AtomicLong();
        LABStats stats = new LABStats(globalHeapCostInBytes);
        LABFiles labFiles = new LABFiles();
        ValueIndex index = createIndex(root, enableHashIndex,
            indexType,
            hashIndexLoadFactor,
            stats,
            null,
            globalHeapCostInBytes);

        long totalCardinality = 100_000_000;

        printLabels();

        String write = stress(true,
            "warm:jit",
            stats,
            labFiles,
            index,
            0,
            totalCardinality,
            800_000, // writesPerSecond
            1_000_000, //writeCount
            1, //readForNSeconds
            1_000_000, // readCount
            false,
            globalHeapCostInBytes); // removes

        List<Future<Object>> futures = index.commit(true, false);
        for (Future<Object> future : futures) {
            future.get();
        }

        System.out.println("\n\n");

        System.out.println(write);
        //System.out.println(read);
        System.out.println("size:" + calculateFileSize(root) + "bytes " + root);
        System.out.println("\n\n");

        System.gc();
        System.runFinalization();

        System.out.println("-------------------------------");

        globalHeapCostInBytes = new AtomicLong();
        stats = new LABStats(globalHeapCostInBytes);
        root = Files.createTempDir();
        index = createIndex(root, enableHashIndex, indexType, hashIndexLoadFactor, stats, labFiles, globalHeapCostInBytes);

        // ---
        System.out.println("Write Stress:");
        printLabels();

        totalCardinality = 1_000_000_000L;

        int threadCount = Runtime.getRuntime().availableProcessors();
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);

        futures = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {

            LABStats stats1 = stats;
            ValueIndex index1 = index;
            long totalCardinality1 = totalCardinality;
            AtomicLong globalHeapCostInBytes1 = globalHeapCostInBytes;
            int efi = i;
            futures.add(executorService.submit(() -> {
                String write1 = stress(true,
                    "stress:RW",
                    stats1,
                    labFiles,
                    index1,
                    (totalCardinality1 / threadCount) * efi,
                    totalCardinality1 / threadCount,
                    100_000, // writesPerSecond
                    (10_000_000), //writeCount
                    1, //readForNSeconds
                    1, // readCount
                    false,
                    globalHeapCostInBytes1); // removes
                return write1;
            }));
        }

        for (Future future : futures) {
            System.out.println(future.get());
        }
        executorService.shutdownNow();


        System.out.println("\n\n");
        ((LAB) index).auditRanges((key) -> "" + UIO.bytesLong(key));
        System.out.println("\n\n");

        System.out.println(write);
        System.out.println("size:" + calculateFileSize(root) + "bytes " + root);
        System.out.println("\n\n");

        System.out.println("COMMIT ALL");
        futures = index.commit(true, true);
        for (Future<Object> future : (futures != null) ? futures : Collections.<Future<Object>>emptyList()) {
            future.get();
        }
        System.out.println("COMMITED ALL");

        System.out.println("COMPACT ALL");
        futures = index.compact(true, 0, 0, true);
        for (Future<Object> future : (futures != null) ? futures : Collections.<Future<Object>>emptyList()) {
            future.get();
        }
        System.out.println("COMPACTED ALL");

        printLabels();

        write = stress(true,
            "stress:R",
            stats,
            labFiles,
            index,
            0,
            totalCardinality,
            0, // writesPerSecond
            0, //writeCount
            10, //readForNSeconds
            100_000_000, // readCount
            false,
            globalHeapCostInBytes); // removes

        System.out.println("\n\n");
        ((LAB) index).auditRanges((key) -> "" + UIO.bytesLong(key));
        System.out.println("\n\n");

        System.out.println(write);
        System.out.println("size:" + calculateFileSize(root) + "bytes " + root);
        System.out.println("\n\n");

    }

    private void printLabels() {

        System.out.println("sample writes writes/sec writeElapse reads reads/sec readElapse hits miss merged split readAmplification approxCount "
            + "debt open closed append journaledAppend merging merged spliting splits slabbed allocationed released freed gc gcCommit "
            + "pressureCommit commit fsyncedCommit bytesWrittenToWAL bytesWrittenAsIndex bytesWrittenAsSplit bytesWrittenAsMerge");

    }

    private ValueIndex createIndex(File root,
        boolean enableHashIndex,
        LABHashIndexType indexType,
        double hashIndexLoadFactor,
        LABStats stats,
        LABFiles labFiles,
        AtomicLong globalHeapCostInBytes) throws Exception {

        System.out.println("Created root " + root);
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100_000, 8);
        LABHeapPressure labHeapPressure = new LABHeapPressure(stats,
            LABEnvironment.buildLABHeapSchedulerThreadPool(1),
            "default",
            1024 * 1024 * 256,
            1024 * 1024 * 512,
            globalHeapCostInBytes,
            LABHeapPressure.FreeHeapStrategy.mostBytesFirst);

        LABEnvironment env = new LABEnvironment(stats,
            labFiles,
            LABEnvironment.buildLABSchedulerThreadPool(1),
            LABEnvironment.buildLABCompactorThreadPool(Math.max(1, Runtime.getRuntime().availableProcessors() - 1)), // compact
            LABEnvironment.buildLABDestroyThreadPool(1), // destroy
            null,
            root, // rootFile
            labHeapPressure,
            4, // minMergeDebt
            8, // maxMergeDebt
            leapsCache,
            new StripingBolBufferLocks(2048),
            true,
            false);

        env.register("8x8fixedWidthRawhide", new LABFixedWidthKeyFixedWidthValueRawhide(8, 8));

        System.out.println("Created env");
        ValueIndex index = env.open(new ValueIndexConfig("foo",
            1024 * 8, // entriesBetweenLeaps
            1024 * 1024 * 512, // maxHeapPressureInBytes
            -1, // splitWhenKeysTotalExceedsNBytes
            -1, // splitWhenValuesTotalExceedsNBytes
            1024 * 1024 * 64, // splitWhenValuesAndKeysTotalExceedsNBytes
            "deprecated",
            "8x8fixedWidthRawhide", //new LABRawhide(),
            MemoryRawEntryFormat.NAME,
            27,
            indexType,
            hashIndexLoadFactor,
            enableHashIndex,
            Long.MAX_VALUE));
        return index;
    }

    private String stress(boolean report,
        String name,
        LABStats stats,
        LABFiles files,
        ValueIndex index,
        long offset,
        long totalCardinality,
        int writesPerSecond,
        int writeCount,
        int readForNSeconds,
        int readCount,
        boolean removes,
        AtomicLong globalHeapCostInBytes) throws Exception {

        AtomicLong version = new AtomicLong();
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


//            if (!files.isEmpty()) {
//                files.take((addedFiles, removedFiles) -> {
//                    System.out.println("Added:" + Lists.newArrayList(addedFiles));
//                    System.out.println("Removed:" + Lists.newArrayList(removedFiles));
//                    return true;
//                });
//            }

            long start = System.currentTimeMillis();
            long writeElapse = 0;
            double writeRate = 0;
            if (writeCount > 0 && totalWrites < writeCount) {
                long preWriteCount = count.get();
                index.append((stream) -> {
                    for (int i = 0; i < writesPerSecond; i++) {
                        count.incrementAndGet();
                        long key = offset + (long) (rand.nextDouble() * totalCardinality);
                        stream.stream(-1,
                            UIO.longBytes(key, keyBytes, 0),
                            System.currentTimeMillis(),
                            (removes) ? rand.nextBoolean() : false,
                            version.incrementAndGet(),
                            UIO.longBytes(value.incrementAndGet(), valuesBytes, 0));
                    }
                    return true;
                }, true, rawEntryBuffer, keyBuffer);

                //index.commit(true);
                long wrote = count.get() - preWriteCount;
                totalWrites += wrote;
                //System.out.println("Commit Elapse:" + (System.currentTimeMillis() - start));
                writeElapse = (System.currentTimeMillis() - start);
                totalWriteTime += writeElapse;
                writeRate = (double) wrote * 1000 / (writeElapse);

            }

            start = System.currentTimeMillis();
            long readElapse = 0;
            double readRate = 0;
            AtomicLong misses = new AtomicLong();
            AtomicLong hits = new AtomicLong();
            if (readCount > 0 && totalReads < readCount) {
                long s = start;

                while (System.currentTimeMillis() - s < (1000 * readForNSeconds)) {

                    index.get((KeyStream keyStream) -> {
                        long k = (long) (rand.nextDouble() * totalCardinality);
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
                readRate = (double) (misses.get() + hits.get()) * 1000 / (readElapse);
                totalHits += hits.get();
                totalMiss += misses.get();
            }

            c++;

            long reads = misses.get() + hits.get();

            LOG.set("writesPerSecond", writesPerSecond);
            LOG.set("writeRate", (long) writeRate);
            LOG.set("writeElapse", writeElapse);
            LOG.set("reads", reads);
            LOG.set("readRate", (long) readRate);
            LOG.set("readElapse", readElapse);
            LOG.set("hits", hits.get());
            LOG.set("misses", misses.get());
            LOG.set("misses", misses.get());

            if (report) {


                System.out.println(name + ":" + c
                    + " " + writesPerSecond
                    + " " + writeRate
                    + " " + writeElapse
                    + " " + reads
                    + " " + readRate
                    + " " + readElapse
                    + " " + hits.getAndSet(0)
                    + " " + misses.getAndSet(0)
                    + " " + index.count()
                    + " " + stats.debt.sumThenReset()
                    + " " + stats.open.sumThenReset()
                    + " " + stats.closed.sumThenReset()
                    + " " + stats.append.sumThenReset()
                    + " " + stats.journaledAppend.sumThenReset()
                    + " " + stats.merging.get()
                    + " " + stats.spliting.get()
                    + " " + stats.slabbed.sumThenReset()
                    + " " + stats.allocated.sumThenReset()
                    + " " + stats.released.sumThenReset()
                    + " " + stats.freed.sumThenReset()
                    + " " + stats.gc.sumThenReset()
                    + " " + stats.gcCommit.sumThenReset()
                    + " " + stats.commit.sumThenReset()
                    + " " + stats.fsyncedCommit.sumThenReset()
                    + " " + stats.bytesWrittenToWAL.sumThenReset()
                    + " " + stats.bytesWrittenAsIndex.sumThenReset()
                    + " " + stats.bytesWrittenAsSplit.sumThenReset()
                    + " " + stats.bytesWrittenAsMerge.sumThenReset()
                );
            }


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

    public static long calculateFileSize(File file) {
        long fileSize = 0L;
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            for (File child : children) {
                fileSize += calculateFileSize(child);
            }
        } else {
            fileSize = file.length();
        }
        return fileSize;
    }
}
