package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.base.BolBuffer;
import com.github.jnthnclt.os.lab.base.UIO;
import com.github.jnthnclt.os.lab.collections.bah.LRUConcurrentBAHLinkedHash;
import com.github.jnthnclt.os.lab.core.LABEnvironment;
import com.github.jnthnclt.os.lab.core.LABHeapPressure;
import com.github.jnthnclt.os.lab.core.LABStats;
import com.github.jnthnclt.os.lab.core.TestUtils;
import com.github.jnthnclt.os.lab.core.api.rawhide.LABRawhide;
import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.guts.allocators.LABAppendOnlyAllocator;
import com.github.jnthnclt.os.lab.core.guts.allocators.LABConcurrentSkipListMap;
import com.github.jnthnclt.os.lab.core.guts.allocators.LABConcurrentSkipListMemory;
import com.github.jnthnclt.os.lab.core.guts.allocators.LABIndexableMemory;
import com.github.jnthnclt.os.lab.core.guts.api.ReadIndex;
import com.github.jnthnclt.os.lab.core.guts.api.Scanner;
import java.io.File;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class IndexNGTest {

    private final Rawhide rawhide = LABRawhide.SINGLETON;

    @Test(enabled = false)
    public void testLeapDisk() throws Exception {

        ExecutorService destroy = Executors.newSingleThreadExecutor();
        File indexFiler = File.createTempFile("l-index", ".tmp");

        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(rawhide.getKeyComparator());

        int count = 16;
        int step = 2;

        IndexRangeId indexRangeId = new IndexRangeId(1, 1, 0);

        LABAppendableIndex write = new LABAppendableIndex(new LongAdder(),
            indexRangeId,
            new AppendOnlyFile(indexFiler),
            64,
            10,
            rawhide,
            TestUtils.indexType,
            0.75d,
            Long.MAX_VALUE
        );

        BolBuffer keyBuffer = new BolBuffer();
        TestUtils.append(new Random(), write, 0, step, count, desired, keyBuffer);
        write.closeAppendable(false);

        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        ReadOnlyIndex leapsAndBoundsIndex = new ReadOnlyIndex(null,
            destroy,
            indexRangeId,
            new ReadOnlyFile(indexFiler),
            rawhide,
            leapsCache);

        assertions(leapsAndBoundsIndex, count, step, desired);
    }

    @Test(enabled = true)
    public void testMemory() throws Exception {

        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(rawhide.getKeyComparator());

        int count = 10;
        int step = 10;

        ExecutorService destroy = Executors.newSingleThreadExecutor();
        AtomicLong globalHeapCostInBytes = new AtomicLong();
        LABStats labStats = new LABStats(globalHeapCostInBytes);
        LABHeapPressure labHeapPressure = new LABHeapPressure(labStats,
            LABEnvironment.buildLABHeapSchedulerThreadPool(1),
            "default",
            -1,
            -1,
            globalHeapCostInBytes,
            LABHeapPressure.FreeHeapStrategy.mostBytesFirst);
        LABMemoryIndex walIndex = new LABMemoryIndex(destroy,
            labHeapPressure,
            labStats,
            rawhide,
            new LABConcurrentSkipListMap(labStats,
                new LABConcurrentSkipListMemory(rawhide,
                    new LABIndexableMemory(new LABAppendOnlyAllocator("test", 2))
                ),
                new StripingBolBufferLocks(1024)
            ));
        BolBuffer keyBuffer = new BolBuffer();
        TestUtils.append(new Random(), walIndex, 0, step, count, desired, keyBuffer);
        assertions(walIndex, count, step, desired);
    }

    @Test(enabled = false)
    public void testMemoryToDisk() throws Exception {

        ExecutorService destroy = Executors.newSingleThreadExecutor();
        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(rawhide.getKeyComparator());

        int count = 10;
        int step = 10;
        AtomicLong globalHeapCostInBytes = new AtomicLong();
        LABStats labStats = new LABStats(globalHeapCostInBytes);
        LABHeapPressure labHeapPressure = new LABHeapPressure(labStats,
            LABEnvironment.buildLABHeapSchedulerThreadPool(1),
            "default",
            -1,
            -1,
            globalHeapCostInBytes,
            LABHeapPressure.FreeHeapStrategy.mostBytesFirst);

        LABMemoryIndex memoryIndex = new LABMemoryIndex(destroy,
            labHeapPressure, labStats, rawhide,
            new LABConcurrentSkipListMap(labStats,
                new LABConcurrentSkipListMemory(rawhide,
                    new LABIndexableMemory(new LABAppendOnlyAllocator("test", 2))
                ),
                new StripingBolBufferLocks(1024)
            ));

        BolBuffer keyBuffer = new BolBuffer();
        TestUtils.append(new Random(), memoryIndex, 0, step, count, desired, keyBuffer);
        assertions(memoryIndex, count, step, desired);

        File indexFiler = File.createTempFile("c-index", ".tmp");
        IndexRangeId indexRangeId = new IndexRangeId(1, 1, 0);
        LABAppendableIndex disIndex = new LABAppendableIndex(new LongAdder(),
            indexRangeId,
            new AppendOnlyFile(indexFiler),
            64,
            10,
            rawhide,
            TestUtils.indexType,
            0.75d,
            Long.MAX_VALUE);
        disIndex.append((stream) -> {
            ReadIndex reader = memoryIndex.acquireReader();
            try {
                Scanner rowScan = reader.rowScan(new BolBuffer(), new BolBuffer());

                BolBuffer rawEntry = new BolBuffer();
                while ((rawEntry = rowScan.next(rawEntry, null)) != null) {
                    byte[] bytes = rawEntry.copy();
                    return stream.stream(new BolBuffer(bytes));
                }
            } finally {
                reader.release();
                reader = null;
            }
            return true;
        }, keyBuffer);
        disIndex.closeAppendable(false);

        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        assertions(new ReadOnlyIndex(null, destroy, indexRangeId, new ReadOnlyFile(indexFiler), rawhide,
            leapsCache), count, step, desired);

    }

    private void assertions(LABMemoryIndex memoryIndex, int count, int step, ConcurrentSkipListMap<byte[], byte[]> desired) throws
        Exception {
        ArrayList<byte[]> keys = new ArrayList<>(desired.navigableKeySet());

        int[] index = new int[1];
        ReadIndex reader = memoryIndex.acquireReader();
        try {
            Scanner rowScan = reader.rowScan(new BolBuffer(), new BolBuffer());


            BolBuffer rawEntry = new BolBuffer();
            while ((rawEntry = rowScan.next(rawEntry, null)) != null) {
                Assert.assertEquals(UIO.bytesLong(keys.get(index[0])), TestUtils.key(rawEntry));
                index[0]++;
            }

        } finally {
            reader.release();
            reader = null;
        }
        //System.out.println("Point Get");
        for (int i = 0; i < count * step; i++) {
            long k = i;
            reader = memoryIndex.acquireReader();
            try {
                byte[] key = UIO.longBytes(k, new byte[8], 0);

                BolBuffer rawEntry = reader.pointScan(true, key);
                if (rawEntry != null) {
                    byte[] rawKey = UIO.longBytes(TestUtils.key(rawEntry), new byte[8], 0);
                    Assert.assertEquals(rawKey, key);
                    byte[] d = desired.get(key);
                    if (d == null) {
                        Assert.fail();
                    } else {
                        Assert.assertEquals(TestUtils.value(rawEntry), TestUtils.value(d));
                    }
                } else {
                    Assert.assertFalse(desired.containsKey(key));
                }

                Assert.assertEquals(rawEntry == null ? false : true, desired.containsKey(key));
            } finally {
                reader.release();
            }
        }

        //System.out.println("Ranges");
        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;

            int[] streamed = new int[1];

            //System.out.println("Asked:" + UIO.bytesLong(keys.get(_i)) + " to " + UIO.bytesLong(keys.get(_i + 3)));
            reader = memoryIndex.acquireReader();
            try {
                Scanner rangeScan = reader.rangeScan(false, false, keys.get(_i), keys.get(_i + 3), new BolBuffer(), new BolBuffer());

                BolBuffer rawEntry = new BolBuffer();
                while ((rawEntry = rangeScan.next(rawEntry, null)) != null) {
                    if (rawEntry != null) {
                        //System.out.println("Streamed:" + TestUtils.toString(entry));
                        streamed[0]++;
                    }
                }
                Assert.assertEquals(3, streamed[0]);
            } finally {
                reader.release();
            }

        }

        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;
            int[] streamed = new int[1];

            reader = memoryIndex.acquireReader();
            try {
                Scanner rangeScan = reader.rangeScan(false,false, UIO.longBytes(UIO.bytesLong(keys.get(_i)) + 1, new byte[8], 0), keys.get(_i + 3),
                    new BolBuffer(),
                    new BolBuffer());

                BolBuffer rawEntry = new BolBuffer();
                while ((rawEntry = rangeScan.next(rawEntry, null)) != null) {
                    if (rawEntry != null) {
                        streamed[0]++;
                    }
                }

                Assert.assertEquals(2, streamed[0]);
            } finally {
                reader.release();
            }

        }
    }

    private void assertions(ReadOnlyIndex walIndex, int count, int step, ConcurrentSkipListMap<byte[], byte[]> desired) throws
        Exception {
        ArrayList<byte[]> keys = new ArrayList<>(desired.navigableKeySet());

        int[] index = new int[1];
        ReadIndex reader = walIndex.acquireReader();
        try {
            Scanner rowScan = reader.rowScan(new BolBuffer(), new BolBuffer());
            BolBuffer rawEntry = new BolBuffer();
            while ((rawEntry = rowScan.next(rawEntry, null)) != null) {
                Assert.assertEquals(TestUtils.key(rawEntry), UIO.bytesLong(keys.get(index[0])));
                index[0]++;
            }

        } finally {
            reader.release();
        }

        System.out.println("Point Get");
        for (int i = 0; i < count * step; i++) {
            long k = i;
            reader = walIndex.acquireReader();
            try {
                byte[] key = UIO.longBytes(k, new byte[8], 0);
                BolBuffer rawEntry = reader.pointScan(true, key);
                if (rawEntry != null) {
                    byte[] rawKey = UIO.longBytes(TestUtils.key(rawEntry), new byte[8], 0);
                    Assert.assertEquals(rawKey, key);
                    byte[] d = desired.get(key);
                    if (d == null) {
                        Assert.fail();
                    } else {
                        Assert.assertEquals(TestUtils.value(rawEntry), TestUtils.value(d));
                    }
                }
                Assert.assertEquals(rawEntry == null ? false : true, desired.containsKey(key));
            } finally {
                reader.release();
            }
        }

        System.out.println("Ranges");
        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;

            int[] streamed = new int[1];

            System.out.println("Asked:" + UIO.bytesLong(keys.get(_i)) + " to " + UIO.bytesLong(keys.get(_i + 3)));
            reader = walIndex.acquireReader();
            try {
                Scanner rangeScan = reader.rangeScan(false, false,keys.get(_i), keys.get(_i + 3), new BolBuffer(), new BolBuffer());
                BolBuffer rawEntry = new BolBuffer();
                while ((rawEntry = rangeScan.next(rawEntry, null)) != null) {
                    System.out.println("Streamed:" + TestUtils.toString(rawEntry));
                    streamed[0]++;
                }
                Assert.assertEquals(3, streamed[0]);
            } finally {
                reader.release();
            }

        }

        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;
            int[] streamed = new int[1];

            reader = walIndex.acquireReader();
            try {
                Scanner rangeScan = reader.rangeScan(false, false, UIO.longBytes(UIO.bytesLong(keys.get(_i)) + 1, new byte[8], 0),
                    keys.get(_i + 3),
                    new BolBuffer(),
                    new BolBuffer());

                BolBuffer rawEntry = new BolBuffer();
                while ((rawEntry = rangeScan.next(rawEntry, null)) != null) {
                    streamed[0]++;
                }
                Assert.assertEquals(2, streamed[0]);
            } finally {
                reader.release();
            }

        }
    }
}
