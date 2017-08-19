package org.colt.os.lab.guts;

import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import java.io.File;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.colt.os.lab.LABEnvironment;
import org.colt.os.lab.LABHeapPressure;
import org.colt.os.lab.LABStats;
import org.colt.os.lab.TestUtils;
import org.colt.os.lab.api.NoOpFormatTransformerProvider;
import org.colt.os.lab.api.RawEntryFormat;
import org.colt.os.lab.api.rawhide.LABRawhide;
import org.colt.os.lab.api.rawhide.Rawhide;
import org.colt.os.lab.guts.allocators.LABAppendOnlyAllocator;
import org.colt.os.lab.guts.allocators.LABConcurrentSkipListMap;
import org.colt.os.lab.guts.allocators.LABConcurrentSkipListMemory;
import org.colt.os.lab.guts.allocators.LABIndexableMemory;
import org.colt.os.lab.guts.api.Next;
import org.colt.os.lab.guts.api.RawEntryStream;
import org.colt.os.lab.guts.api.ReadIndex;
import org.colt.os.lab.guts.api.Scanner;
import org.colt.os.lab.io.BolBuffer;
import org.colt.os.lab.io.api.UIO;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class IndexNGTest {

    private final Rawhide rawhide = LABRawhide.SINGLETON;

    @Test(enabled = true)
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
            new RawEntryFormat(0, 0),
            NoOpFormatTransformerProvider.NO_OP,
            TestUtils.indexType,
            0.75d
        );

        BolBuffer keyBuffer = new BolBuffer();
        TestUtils.append(new Random(), write, 0, step, count, desired, keyBuffer);
        write.closeAppendable(false);

        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        ReadOnlyIndex leapsAndBoundsIndex = new ReadOnlyIndex(destroy,
            indexRangeId,
            new ReadOnlyFile(indexFiler),
            NoOpFormatTransformerProvider.NO_OP,
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
        LABStats labStats = new LABStats();
        LABHeapPressure labHeapPressure = new LABHeapPressure(labStats,
            LABEnvironment.buildLABHeapSchedulerThreadPool(1),
            "default",
            -1,
            -1,
            new AtomicLong(),
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
        LABStats labStats = new LABStats();
        LABHeapPressure labHeapPressure = new LABHeapPressure(labStats,
            LABEnvironment.buildLABHeapSchedulerThreadPool(1),
            "default",
            -1,
            -1,
            new AtomicLong(),
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
            new RawEntryFormat(0, 0),
            NoOpFormatTransformerProvider.NO_OP,
            TestUtils.indexType,
            0.75d);
        disIndex.append((stream) -> {
            ReadIndex reader = memoryIndex.acquireReader();
            try {
                Scanner rowScan = reader.rowScan(new ActiveScan(false), new BolBuffer(), new BolBuffer());
                RawEntryStream rawStream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                    byte[] bytes = rawEntry.copy();
                    return stream.stream(readKeyFormatTransformer, readValueFormatTransformer, new BolBuffer(bytes));
                };
                while (rowScan.next(rawStream) == Next.more) {
                }
            } finally {
                reader.release();
                reader = null;
            }
            return true;
        }, keyBuffer);
        disIndex.closeAppendable(false);

        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        assertions(new ReadOnlyIndex(destroy, indexRangeId, new ReadOnlyFile(indexFiler), NoOpFormatTransformerProvider.NO_OP, rawhide,
            leapsCache), count, step, desired);

    }

    private void assertions(LABMemoryIndex memoryIndex, int count, int step, ConcurrentSkipListMap<byte[], byte[]> desired) throws
        Exception {
        ArrayList<byte[]> keys = new ArrayList<>(desired.navigableKeySet());

        int[] index = new int[1];
        ReadIndex reader = memoryIndex.acquireReader();
        try {
            Scanner rowScan = reader.rowScan(new ActiveScan(false), new BolBuffer(), new BolBuffer());
            RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                System.out.println("rowScan:" + TestUtils.key(rawEntry));
                Assert.assertEquals(UIO.bytesLong(keys.get(index[0])), TestUtils.key(rawEntry));
                index[0]++;
                return true;
            };
            while (rowScan.next(stream) == Next.more) {
            }
        } finally {
            reader.release();
            reader = null;
        }
        System.out.println("Point Get");
        for (int i = 0; i < count * step; i++) {
            long k = i;
            reader = memoryIndex.acquireReader();
            try {
                byte[] key = UIO.longBytes(k, new byte[8], 0);
                RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {

                    System.out.println("Got: " + TestUtils.toString(rawEntry));
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
                    return rawEntry != null;
                };
                Scanner scanner = reader.pointScan(new ActiveScan(true), key, new BolBuffer(), new BolBuffer());
                if (scanner != null){
                    scanner.next(stream);
                }

                Assert.assertEquals(scanner == null ? false : true, desired.containsKey(key));
            } finally {
                reader.release();
            }
        }

        System.out.println("Ranges");
        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;

            int[] streamed = new int[1];
            RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, entry) -> {
                if (entry != null) {
                    System.out.println("Streamed:" + TestUtils.toString(entry));
                    streamed[0]++;
                }
                return true;
            };

            System.out.println("Asked:" + UIO.bytesLong(keys.get(_i)) + " to " + UIO.bytesLong(keys.get(_i + 3)));
            reader = memoryIndex.acquireReader();
            try {
                Scanner rangeScan = reader.rangeScan(new ActiveScan(false), keys.get(_i), keys.get(_i + 3), new BolBuffer(), new BolBuffer());
                while (rangeScan != null && rangeScan.next(stream) == Next.more) {
                }
                Assert.assertEquals(3, streamed[0]);
            } finally {
                reader.release();
            }

        }

        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;
            int[] streamed = new int[1];
            RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, entry) -> {
                if (entry != null) {
                    streamed[0]++;
                }
                return TestUtils.value(entry) != -1;
            };
            reader = memoryIndex.acquireReader();
            try {
                Scanner rangeScan = reader.rangeScan(new ActiveScan(false), UIO.longBytes(UIO.bytesLong(keys.get(_i)) + 1, new byte[8], 0), keys.get(_i + 3),
                    new BolBuffer(),
                    new BolBuffer());
                while (rangeScan != null && rangeScan.next(stream) == Next.more) {
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
            Scanner rowScan = reader.rowScan(new ActiveScan(false), new BolBuffer(), new BolBuffer());
            RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                System.out.println("rowScan: found:" + TestUtils.key(rawEntry) + " expected:" + UIO.bytesLong(keys.get(index[0])));
                Assert.assertEquals(TestUtils.key(rawEntry), UIO.bytesLong(keys.get(index[0])));
                index[0]++;
                return true;
            };
            while (rowScan.next(stream) == Next.more) {
            }
        } finally {
            reader.release();
            reader = null;
        }
        System.out.println("Point Get");
        for (int i = 0; i < count * step; i++) {
            long k = i;
            reader = walIndex.acquireReader();
            try {
                byte[] key = UIO.longBytes(k, new byte[8], 0);
                RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {

                    System.out.println("Got: " + TestUtils.toString(rawEntry));
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
                    return rawEntry != null;
                };
                Scanner scanner = reader.pointScan(new ActiveScan(true), key, new BolBuffer(), new BolBuffer());
                if (scanner != null){
                    scanner.next(stream);
                }

                Assert.assertEquals(scanner == null ? false : true, desired.containsKey(key));
            } finally {
                reader.release();
            }
        }

        System.out.println("Ranges");
        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;

            int[] streamed = new int[1];
            RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, entry) -> {
                if (entry != null) {
                    System.out.println("Streamed:" + TestUtils.toString(entry));
                    streamed[0]++;
                }
                return true;
            };

            System.out.println("Asked:" + UIO.bytesLong(keys.get(_i)) + " to " + UIO.bytesLong(keys.get(_i + 3)));
            reader = walIndex.acquireReader();
            try {
                Scanner rangeScan = reader.rangeScan(new ActiveScan(false), keys.get(_i), keys.get(_i + 3), new BolBuffer(), new BolBuffer());
                while (rangeScan != null && rangeScan.next(stream) == Next.more) {
                }
                Assert.assertEquals(3, streamed[0]);
            } finally {
                reader.release();
            }

        }

        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;
            int[] streamed = new int[1];
            RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, entry) -> {
                if (entry != null) {
                    streamed[0]++;
                }
                return TestUtils.value(entry) != -1;
            };
            reader = walIndex.acquireReader();
            try {
                Scanner rangeScan = reader.rangeScan(new ActiveScan(false), UIO.longBytes(UIO.bytesLong(keys.get(_i)) + 1, new byte[8], 0), keys.get(_i + 3),
                    new BolBuffer(),
                    new BolBuffer());
                while (rangeScan != null && rangeScan.next(stream) == Next.more) {
                }
                Assert.assertEquals(2, streamed[0]);
            } finally {
                reader.release();
            }

        }
    }
}
