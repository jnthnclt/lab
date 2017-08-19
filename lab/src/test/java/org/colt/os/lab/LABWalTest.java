package org.colt.os.lab;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import java.io.File;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.colt.os.lab.LABEnvironmentNGTest.IdProvider;
import org.colt.os.lab.api.MemoryRawEntryFormat;
import org.colt.os.lab.api.NoOpFormatTransformerProvider;
import org.colt.os.lab.api.ValueIndex;
import org.colt.os.lab.api.ValueIndexConfig;
import org.colt.os.lab.api.rawhide.LABRawhide;
import org.colt.os.lab.guts.Leaps;
import org.colt.os.lab.guts.StripingBolBufferLocks;
import org.colt.os.lab.io.BolBuffer;
import org.colt.os.lab.io.api.UIO;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Created by jonathan.colt on 3/3/17.
 */
public class LABWalTest {


    @Test(enabled = false, description = "Slightly obnoxious")
    public void testConcurrency() throws Exception {
        long maxWALSizeInBytes = 1024L;
        long maxEntriesPerWAL = 1_000L;
        long maxEntrySizeInBytes = 128L;

        LABStats labStats = new LABStats(1, 0, 1000L);
        File envRoot = Files.createTempDir();
        LABHeapPressure LABHeapPressure1 = new LABHeapPressure(labStats,
            LABEnvironment.buildLABHeapSchedulerThreadPool(1),
            "default",
            1024 * 10,
            1024 * 10,
            new AtomicLong(),
            LABHeapPressure.FreeHeapStrategy.mostBytesFirst);
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        LABEnvironment env = new LABEnvironment(labStats,
            LABEnvironment.buildLABSchedulerThreadPool(1),
            LABEnvironment.buildLABCompactorThreadPool(4),
            LABEnvironment.buildLABDestroyThreadPool(1),
            new LABWALConfig("wal",
                "meta",
                maxWALSizeInBytes,
                maxEntriesPerWAL,
                maxEntrySizeInBytes,
                -1),
            envRoot,
            LABHeapPressure1,
            4,
            8,
            leapsCache,
            new StripingBolBufferLocks(1024),
            true,
            false);

        System.out.println("Opening...");
        env.open();

        int numValueIndexes = 1;
        @SuppressWarnings("unchecked")
        ValueIndex<byte[]>[] valueIndexes = new ValueIndex[numValueIndexes];
        //byte[][] valueIndexIds = new byte[numValueIndexes][];
        for (int i = 0; i < numValueIndexes; i++) {
            String name = "index-" + i;
            ValueIndexConfig valueIndexConfig = new ValueIndexConfig(name, 4096, 1024 * 1024 * 10, -1, -1, -1,
                NoOpFormatTransformerProvider.NAME, LABRawhide.NAME, MemoryRawEntryFormat.NAME, 2, TestUtils.indexType, 0.75d, false);
            valueIndexes[i] = env.open(valueIndexConfig);
            //valueIndexIds[i] = name.getBytes(StandardCharsets.UTF_8);
        }

        int concurrencyLevel = 8;
        int numKeys = 100;
        int maxWrites = 10_000;

        AtomicBoolean running = new AtomicBoolean(true);
        ConcurrentMap<Long, Long>[] keyTimestamps = new ConcurrentMap[numValueIndexes];
        AtomicLong[] payloads = new AtomicLong[numValueIndexes];
        for (int i = 0; i < numValueIndexes; i++) {
            payloads[i] = new AtomicLong(0);
            keyTimestamps[i] = Maps.newConcurrentMap();
        }


        System.out.println("Running...");
        ExecutorService executorService = Executors.newFixedThreadPool(concurrencyLevel);
        List<Future<?>> futures = Lists.newArrayList();
        for (int i = 0; i < concurrencyLevel; i++) {
            int index = i;
            futures.add(executorService.submit(() -> {
                int count = 0;
                while ((running.get() || count < numKeys) && count < maxWrites) {
                    if (count % 1_000 == 0) {
                        System.out.println("Thread:" + index + " count:" + count);
                    }
                    count++;

                    for (int vi = 0; vi < numValueIndexes; vi++) {
                        ValueIndex<byte[]> valueIndex = valueIndexes[vi];
                        payloads[vi].incrementAndGet();
                        long key = (count % numKeys);
                        byte[] bytes = UIO.longBytes(key, new byte[8], 0);
                        long timestamp = Integer.MAX_VALUE - count;
                        long version = timestamp + 1;
                        keyTimestamps[vi].merge(key, timestamp, Math::max);
                        valueIndex.append(stream -> {
                            return stream.stream(0, bytes, timestamp, false, version, bytes);
                        }, false, new BolBuffer(), new BolBuffer());
                    }
                    Thread.yield();
                }
                return true;
            }));
        }

        Thread.sleep(5_000L);
        running.set(false);

        System.out.println("Stopping...");
        for (Future<?> future : futures) {
            future.get();
        }

        System.out.println("Closing...");
        for (ValueIndex<byte[]> valueIndex : valueIndexes) {
            valueIndex.close(true, false);
        }
        env.close();
        executorService.shutdownNow();

        System.out.println("Waiting...");
        Thread.sleep(2_000L);

        System.out.println("Reopening...");
        env = new LABEnvironment(labStats,
            LABEnvironment.buildLABSchedulerThreadPool(1),
            LABEnvironment.buildLABCompactorThreadPool(4),
            LABEnvironment.buildLABDestroyThreadPool(1),
            new LABWALConfig("wal",
                "meta",
                maxWALSizeInBytes,
                maxEntriesPerWAL,
                maxEntrySizeInBytes,
                -1),
            envRoot,
            LABHeapPressure1,
            4,
            8,
            leapsCache,
            new StripingBolBufferLocks(1024),
            true,
            false);
        env.open((valueIndexId, key, timestamp, tombstoned, version, payload) -> {
            System.out.println("Applied");
            return true;
        });
        for (int i = 0; i < numValueIndexes; i++) {
            String name = "index-" + i;
            ValueIndexConfig valueIndexConfig = new ValueIndexConfig(name, 4096, 1024 * 1024 * 10, -1, -1, -1,
                NoOpFormatTransformerProvider.NAME, LABRawhide.NAME, MemoryRawEntryFormat.NAME, 2, TestUtils.indexType, 0.75d, false);
            valueIndexes[i] = env.open(valueIndexConfig);
            //valueIndexIds[i] = name.getBytes(StandardCharsets.UTF_8);
        }

        System.out.println("Validating...");
        AtomicLong[] journalCount = new AtomicLong[numValueIndexes];
        for (int i = 0; i < numValueIndexes; i++) {
            journalCount[i] = new AtomicLong(0);
        }
        for (int i = 0; i < numValueIndexes; i++) {
            int index = i;
            AtomicLong count = new AtomicLong(0);
            AtomicLong missed = new AtomicLong(0);
            valueIndexes[i].get(
                keyStream -> {
                    for (int j = 0; j < numKeys; j++) {
                        byte[] bytes = UIO.longBytes(j, new byte[8], 0);
                        keyStream.key(j, bytes, 0, 8);
                    }
                    return true;
                },
                (index1, key, timestamp, tombstoned, version, payload) -> {
                    if (key != null && timestamp >= 0) {
                        count.incrementAndGet();
                        Long expected = keyTimestamps[index].get(key.getLong(0));
                        if (expected == null || expected != timestamp) {
                            System.out.println("Index:" + index + " expected:" + expected + " found:" + timestamp);
                        }
                    } else {
                        System.out.println("Index:" + index + " missed");
                        missed.incrementAndGet();
                    }
                    return true;
                },
                false);
            System.out.println("Index:" + index + " count:" + count + " missed:" + missed);
        }
    }

    private File testEnvRoot;

    @BeforeTest
    public void beforeTest() {
        testEnvRoot = Files.createTempDir();
    }

    @Test(invocationCount = 2, singleThreaded = true)
    public void testEnvWithWALAndMemMap() throws Exception {

        File root = testEnvRoot;
        System.out.println("Created root " + root);
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        LABHeapPressure LABHeapPressure1 = new LABHeapPressure(new LABStats(),
            LABEnvironment.buildLABHeapSchedulerThreadPool(1),
            "default",
            1024,
            1024 * 1024 * 10,
            new AtomicLong(),
            LABHeapPressure.FreeHeapStrategy.mostBytesFirst);

        LABEnvironment env = new LABEnvironment(new LABStats(),
            LABEnvironment.buildLABSchedulerThreadPool(1),
            LABEnvironment.buildLABCompactorThreadPool(4),
            LABEnvironment.buildLABDestroyThreadPool(1),
            new LABWALConfig("labWal",
                "labMeta",
                1024 * 1024 * 10,
                10000,
                1024 * 1024 * 10,
                1024 * 1024 * 10),
            root,
            LABHeapPressure1, 4, 8, leapsCache,
            new StripingBolBufferLocks(1024),
            true,
            true);

        ValueIndexConfig valueIndexConfig = new ValueIndexConfig("foo", 4096, 1024 * 1024 * 10, -1, -1, -1,
            NoOpFormatTransformerProvider.NAME, LABRawhide.NAME, MemoryRawEntryFormat.NAME, 19, TestUtils.indexType, 2d, true);

        System.out.println("Created env");

        String[] lastJournal = { null };
        env.open((valueIndexId, key, timestamp, tombstoned, version, payload) -> {
            String m = " key:" + UIO.bytesLong(key) + " timestamp:" + timestamp + " version:" + UIO.bytesLong(payload);
            if (lastJournal[0] == null) {
                System.out.println("First: " + m);
            }
            lastJournal[0] = m;
            return true;
        });
        System.out.println("Last: " + lastJournal[0]);

        ValueIndex index = env.open(valueIndexConfig);

        AtomicInteger monotonic = new AtomicInteger(-1);
        String[] lastWTF = { "" };
        index.rowScan((index1, key, timestamp, tombstoned, version, payload) -> {

            if (monotonic.get() + 1 != payload.getLong(0) || monotonic.get() + 1 != timestamp) {
                System.out.println(lastWTF[0]);
                System.out.println("opening:" + (monotonic.get() + 1) + " vs " + payload.getLong(0) + " t:" + timestamp);
            }
            lastWTF[0] = "opening:" + (monotonic.get() + 1) + " vs " + payload.getLong(0) + " t:" + timestamp;
            Assert.assertEquals(monotonic.get() + 1, payload.getLong(0), "unexpected payload");
            Assert.assertEquals(monotonic.get() + 1, timestamp, "unexpected timestamp");
            monotonic.set((int) payload.getLong(0));
            return true;
        }, true);


        if (monotonic.get() == -1) {
            monotonic.set(0);
        }

        System.out.println("Opened at monotonic:" + monotonic.get());

        IdProvider idProvider = new IdProvider() {
            @Override
            public int nextId() {
                return monotonic.getAndIncrement();
            }

            @Override
            public void reset() {
                monotonic.set(0);
            }
        };

        int batchCount = 100;
        int batchSize = 1_000;
        if (monotonic.get() != 0) {
            batchCount = 1;
        }

        System.out.println("Open env");
        index(index, "foo", idProvider, batchCount, batchSize, false);
        System.out.println("Indexed");

        AtomicInteger all = new AtomicInteger();
        index.rowScan((index1, key, timestamp, tombstoned, version, payload) -> {
            all.set((int) payload.getLong(0));
            return true;
        }, true);

        System.out.println();
        System.out.println("/----------------------------");
        System.out.println("| Finally:" + all.get() + " " + root);
        System.out.println("\\----------------------------");
        System.out.println();


    }

    private void index(ValueIndex index,
        String name,
        IdProvider idProvider,
        int commitCount,
        int batchCount,
        boolean commit) throws Exception {


        AtomicLong count = new AtomicLong();
        BolBuffer rawEntryBuffer = new BolBuffer();
        BolBuffer keyBuffer = new BolBuffer();
        for (int c = 0; c < commitCount; c++) {
            long start = System.currentTimeMillis();
            int[] wroteV = new int[1];
            index.append((stream) -> {
                for (int i = 0; i < batchCount; i++) {
                    count.incrementAndGet();
                    int nextI = idProvider.nextId();
                    int nextV = nextI;
                    wroteV[0] = nextV;
                    stream.stream(-1,
                        UIO.longBytes(nextI, new byte[8], 0),
                        nextI,
                        false,
                        0,
                        UIO.longBytes(nextV, new byte[8], 0));
                }
                return true;
            }, true, rawEntryBuffer, keyBuffer);

            System.out.println("---->   Append Elapse:" + (System.currentTimeMillis() - start) + " " + wroteV[0]);
            if (commit) {
                start = System.currentTimeMillis();
                index.commit(true, true);
                System.out.println("----> Commit Elapse:" + (System.currentTimeMillis() - start));
            }
        }
    }

}
