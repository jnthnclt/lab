package com.github.jnthnclt.os.lab.core;

import com.github.jnthnclt.os.lab.base.BolBuffer;
import com.github.jnthnclt.os.lab.base.UIO;
import com.github.jnthnclt.os.lab.collections.bah.LRUConcurrentBAHLinkedHash;
import com.github.jnthnclt.os.lab.core.api.exceptions.LABClosedException;
import com.github.jnthnclt.os.lab.core.api.rawhide.LABRawhide;
import com.github.jnthnclt.os.lab.core.guts.LABMapProvider;
import com.github.jnthnclt.os.lab.core.guts.Leaps;
import com.github.jnthnclt.os.lab.core.guts.StripingBolBufferLocks;
import com.github.jnthnclt.os.lab.core.guts.allocators.LABAppendOnlyAllocator;
import com.github.jnthnclt.os.lab.core.guts.allocators.LABConcurrentSkipListMap;
import com.github.jnthnclt.os.lab.core.guts.allocators.LABConcurrentSkipListMemory;
import com.github.jnthnclt.os.lab.core.guts.allocators.LABIndexableMemory;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class LABValidationNGTest {

    @Test(enabled = true, invocationCount = 1, singleThreaded = true)
    public void testClose() throws Exception {

        ExecutorService compact = Executors.newFixedThreadPool(2,
            new ThreadFactoryBuilder().setNameFormat("lab-compact-%d").build());

        ExecutorService destroy = Executors.newFixedThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("lab-destroy-%d").build());

        ExecutorService scheduler = LABEnvironment.buildLABSchedulerThreadPool(1);

        File walRoot = com.google.common.io.Files.createTempDir();
        File root = com.google.common.io.Files.createTempDir();
        File finalRoot = com.google.common.io.Files.createTempDir();
        int entriesBetweenLeaps = 2;
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        AtomicLong globalHeapCostInBytes = new AtomicLong();
        LABStats labStats = new LABStats(globalHeapCostInBytes);
        LABHeapPressure labHeapPressure = new LABHeapPressure(labStats,
            LABEnvironment.buildLABHeapSchedulerThreadPool(1),
            "default",
            1024 * 1024 * 5,
            1024 * 1024 * 10,
            globalHeapCostInBytes,
            LABHeapPressure.FreeHeapStrategy.mostBytesFirst);

        StripingBolBufferLocks stripingBolBufferLocks = new StripingBolBufferLocks(1024);

        LABWAL wal = new LABWAL(labStats, walRoot, 1024 * 1024 * 10, 1000, 1024 * 1024 * 10, 1024 * 1024 * 10);

        LABMapProvider indexProvider = (rawhide1, poweredUpTo) -> {
            LABAppendOnlyAllocator allocator = new LABAppendOnlyAllocator("test", Math.max(3, poweredUpTo));
            LABIndexableMemory memory = new LABIndexableMemory(allocator);
            LABConcurrentSkipListMemory skipList = new LABConcurrentSkipListMemory(rawhide1, memory);
            return new LABConcurrentSkipListMap(labStats, skipList, stripingBolBufferLocks);
        };

        LAB lab = new LAB(labStats,
            LABRawhide.NAME,
            LABRawhide.SINGLETON,
            scheduler,
            compact,
            destroy,
            root,
            wal,
            "lab".getBytes(),
            "lab",
            entriesBetweenLeaps,
            labHeapPressure,
            1024 * 1024 * 10,
            4,
            8,
            128,
            0,
            0,
            leapsCache,
            indexProvider,
            false,
            TestUtils.indexType,
            0.75d,
            true,
            () -> 0,
            null);

        int writerCount = 12;
        ExecutorService writers = Executors.newFixedThreadPool(writerCount, new ThreadFactoryBuilder().setNameFormat("writers-%d").build());
        int commitCount = 100;
        int batchSize = 1;
        AtomicLong value = new AtomicLong();
        AtomicLong version = new AtomicLong();
        AtomicLong count = new AtomicLong();
        boolean fsync = true;
        AtomicBoolean close = new AtomicBoolean(false);

        AtomicLong nextId = new AtomicLong();
        AtomicLong running = new AtomicLong();
        List<Future> writerFutures = new ArrayList<>();
        for (int i = 0; i < writerCount; i++) {
            int wi = i;
            running.incrementAndGet();
            writerFutures.add(writers.submit(() -> {
                try {
                    BolBuffer rawEntryBuffer = new BolBuffer();
                    BolBuffer keyBuffer = new BolBuffer();
                    for (int c = 0; c < commitCount; c++) {

                        if (version.get() > (writerCount * commitCount * batchSize) / 2) {
                            if (close.compareAndSet(false, true)) {
                                //System.out.println("****** Closing lab during writes... ****** ");
                                lab.close(true, fsync);
                                //System.out.println("****** Lab closed... ****** ");

                                try {
                                    FileUtils.forceMkdir(new File(finalRoot, "foobar"));
                                    Files.move(root.toPath(), new File(finalRoot, "foobar").toPath(), StandardCopyOption.ATOMIC_MOVE);
                                } catch (Exception x) {
                                    Assert.fail();
                                }
                                //System.out.println("****** Lab moved... ****** ");
                            }
                        }
                        lab.append((stream) -> {
                            for (int b = 0; b < batchSize; b++) {
                                count.incrementAndGet();
                                stream.stream(-1,
                                    UIO.longBytes(nextId.incrementAndGet(), new byte[8], 0),
                                    System.currentTimeMillis(),
                                    false,
                                    version.incrementAndGet(),
                                    UIO.longBytes(value.incrementAndGet(), new byte[8], 0));
                            }
                            return true;
                        }, fsync, rawEntryBuffer, keyBuffer);

                        lab.commit(fsync, true);
                    }
                    //System.out.println("Writer " + wi + " done...");
                    return null;
                } catch (Exception x) {
                    if (close.get() && (x instanceof LABClosedException)) {
                        //System.out.println("Writer " + wi + " exiting because: " + x);
                        return null;
                    } else {
                        x.printStackTrace();
                        throw x;
                    }
                } finally {
                    running.decrementAndGet();
                }
            }));
        }

        for (Future f : writerFutures) {
            f.get();
        }
        writers.shutdownNow();
        scheduler.shutdownNow();
        compact.shutdownNow();
        destroy.shutdownNow();

    }

    @Test(enabled = true, invocationCount = 1, singleThreaded = true)
    public void testConcurrencyMethod() throws Exception {

        ExecutorService compact = Executors.newFixedThreadPool(2,
            new ThreadFactoryBuilder().setNameFormat("lab-compact-%d").build());

        ExecutorService destroy = Executors.newFixedThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("lab-destroy-%d").build());

        ExecutorService scheduler = LABEnvironment.buildLABSchedulerThreadPool(1);

        File walRoot = com.google.common.io.Files.createTempDir();
        File root = com.google.common.io.Files.createTempDir();
        int entriesBetweenLeaps = 2;
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        AtomicLong globalHeapCostInBytes = new AtomicLong();
        LABStats labStats = new LABStats(globalHeapCostInBytes);
        LABHeapPressure labHeapPressure = new LABHeapPressure(labStats,
            LABEnvironment.buildLABHeapSchedulerThreadPool(1),
            "default",
            1024 * 1024 * 5,
            1024 * 1024 * 10,
            globalHeapCostInBytes,
            LABHeapPressure.FreeHeapStrategy.mostBytesFirst);

        LABWAL wal = new LABWAL(labStats, walRoot, 1024 * 1024 * 10, 1000, 1024 * 1024 * 10, 1024 * 1024 * 10);

        StripingBolBufferLocks stripingBolBufferLocks = new StripingBolBufferLocks(1024);

        LABRawhide rawhide = LABRawhide.SINGLETON;

        LABMapProvider indexProvider = (rawhide1, poweredUpTo) -> {
            LABAppendOnlyAllocator allocator = new LABAppendOnlyAllocator("test", Math.max(3, poweredUpTo));
            LABIndexableMemory memory = new LABIndexableMemory(allocator);
            LABConcurrentSkipListMemory skipList = new LABConcurrentSkipListMemory(rawhide1, memory);
            return new LABConcurrentSkipListMap(labStats, skipList, stripingBolBufferLocks);
        };

        LAB lab = new LAB(
            labStats,
            LABRawhide.NAME, rawhide,
            scheduler,
            compact,
            destroy,
            root,
            wal,
            "lab".getBytes(),
            "lab",
            entriesBetweenLeaps,
            labHeapPressure,
            1024 * 1024 * 10,
            4, 8, 128, 0, 0,
            leapsCache,
            indexProvider,
            false,
            TestUtils.indexType,
            0.75d,
            true,
            () -> 0,
            null);

        validationTest(lab);

        lab.close(true, true);
        scheduler.shutdownNow();
        compact.shutdownNow();
        destroy.shutdownNow();

    }

    private void validationTest(LAB lab) throws InterruptedException, Exception, ExecutionException {
        int writerCount = 30;
        int readerCount = 2;

        AtomicLong hits = new AtomicLong();
        AtomicLong version = new AtomicLong();
        AtomicLong value = new AtomicLong();
        AtomicLong count = new AtomicLong();

        int commitCount = 30;
        int batchSize = 1;
        boolean fsync = true;

        ExecutorService writers = Executors.newFixedThreadPool(writerCount, new ThreadFactoryBuilder().setNameFormat("writers-%d").build());
        ExecutorService readers = Executors.newFixedThreadPool(readerCount, new ThreadFactoryBuilder().setNameFormat("readers-%d").build());

        AtomicLong nextId = new AtomicLong();
        AtomicLong running = new AtomicLong();
        List<Future> writerFutures = new ArrayList<>();
        for (int i = 0; i < writerCount; i++) {
            running.incrementAndGet();
            writerFutures.add(writers.submit(() -> {
                try {
                    BolBuffer rawEntryBuffer = new BolBuffer();
                    BolBuffer keyBuffer = new BolBuffer();
                    for (int c = 0; c < commitCount; c++) {
                        lab.append((stream) -> {
                            for (int b = 0; b < batchSize; b++) {
                                count.incrementAndGet();
                                stream.stream(-1,
                                    UIO.longBytes(nextId.incrementAndGet(), new byte[8], 0),
                                    System.currentTimeMillis(),
                                    false,
                                    version.incrementAndGet(),
                                    UIO.longBytes(value.incrementAndGet(), new byte[8], 0));
                            }
                            return true;
                        }, fsync, rawEntryBuffer, keyBuffer);
                        lab.commit(fsync, true);
                        //System.out.println((c + 1) + " out of " + commitCount + " gets:" + hits.get() + " debt:" + lab.debt());
                    }
                    return null;
                } catch (Exception x) {
                    x.printStackTrace();
                    throw x;
                } finally {
                    running.decrementAndGet();
                }
            }));
        }

        AtomicLong passed = new AtomicLong();
        AtomicLong failed = new AtomicLong();
        List<String> log = new ArrayList<>();

        List<Future> readerFutures = new ArrayList<>();
        for (int r = 0; r < readerCount; r++) {
            int readerId = r;
            readerFutures.add(readers.submit(() -> {
                try {
                    int overRun = 25;
                    while (running.get() > 0 || overRun > 0) {
                        long maxId = nextId.get();
                        if (maxId == 0) {
                            Thread.yield();
                            continue;
                        }
                        Set<Long> found = new HashSet<>();
                        for (int i = 1; i <= maxId; i++) {
                            int ii = i;
                            lab.get(
                                (keyStream) -> {
                                    byte[] key = UIO.longBytes(ii, new byte[8], 0);
                                    keyStream.key(0, key, 0, key.length);
                                    return true;
                                },
                                (index, key, timestamp, tombstoned, version1, value1) -> {
                                    hits.incrementAndGet();
                                    found.add(key == null ? 0L : key.getLong(0));
                                    return true;
                                }, true);
                        }

                        if (maxId == found.size()) {
                            passed.incrementAndGet();
                            //System.out.println("PASSED: " + found.size() + "  vs " + maxId);
                            log.add("PASSED: " + found.size() + "  vs " + maxId);
                            failed.set(0);
                        } else {
                            failed.incrementAndGet();
                            List<Long> missing = new ArrayList<>();
                            for (long i = 1; i <= maxId; i++) {
                                if (!found.contains(i)) {
                                    missing.add(i);
                                }
                            }
                            //System.out.println("FAILED: " + found.size() + "  vs " + maxId + " missing=" + missing + " " + missing.size());
                            log.add("FAILED: " + found.size() + "  vs " + maxId + " missing=" + missing + " " + missing.size());
                        }
                        if (running.get() <= 0) {
                            overRun--;
                            Thread.sleep(10);
                        }

                    }
                    //System.out.println("Reader (" + readerId + ") finished.");
                    return null;
                } catch (Exception x) {
                    log.add(x.getMessage());
                    x.printStackTrace();
                    throw x;
                }
            }));
        }

        for (Future f : writerFutures) {
            f.get();
        }

        for (Future f : readerFutures) {
            f.get();
        }
        System.out.println("------------------------- READERS ARE DONE -----------------------");

        System.out.println("------------------------------------------------------------------");
        System.out.println("------------------------- ALL DONE -------------------------------");
        System.out.println("------------------------------------------------------------------");

        writers.shutdownNow();
        readers.shutdownNow();

        if (failed.get() > 0) {
            for (String failure : log) {
                System.out.println(":( " + failure);
            }
            Assert.fail();
        }
    }

}
