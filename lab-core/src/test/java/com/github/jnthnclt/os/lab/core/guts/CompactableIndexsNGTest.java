package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.collections.bah.LRUConcurrentBAHLinkedHash;
import com.github.jnthnclt.os.lab.core.api.FormatTransformer;
import com.github.jnthnclt.os.lab.core.api.rawhide.LABRawhide;
import com.github.jnthnclt.os.lab.core.guts.api.Next;
import java.io.File;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import com.github.jnthnclt.os.lab.core.LABEnvironment;
import com.github.jnthnclt.os.lab.core.LABStats;
import com.github.jnthnclt.os.lab.core.TestUtils;
import com.github.jnthnclt.os.lab.core.api.MemoryRawEntryFormat;
import com.github.jnthnclt.os.lab.core.api.NoOpFormatTransformerProvider;
import com.github.jnthnclt.os.lab.core.api.RawEntryFormat;
import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.guts.api.RawEntryStream;
import com.github.jnthnclt.os.lab.core.guts.api.ReadIndex;
import com.github.jnthnclt.os.lab.core.guts.api.Scanner;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import com.github.jnthnclt.os.lab.core.io.api.UIO;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class CompactableIndexsNGTest {

    private final Rawhide rawhide = LABRawhide.SINGLETON;

    @Test(enabled = true)
    public void testPointGets() throws Exception {

        ExecutorService destroy = Executors.newSingleThreadExecutor();
        CompactableIndexes indexs = new CompactableIndexes(new LABStats(), rawhide);
        AtomicLong id = new AtomicLong();
        AtomicLong timeProvider = new AtomicLong();
        BolBuffer keyBuffer = new BolBuffer();

        int[] counts = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 };
        for (int wi = 0; wi < counts.length; wi++) {
            int ci = wi;
            File file = File.createTempFile("a-index-" + wi, ".tmp");
            AppendOnlyFile appendOnlyFile = new AppendOnlyFile(file);
            IndexRangeId indexRangeId = new IndexRangeId(wi, wi, 0);

            int entriesBetweenLeaps = 2;
            int maxLeaps = RangeStripedCompactableIndexes.calculateIdealMaxLeaps(counts[ci], entriesBetweenLeaps);
            LABAppendableIndex write = new LABAppendableIndex(new LongAdder(),
                indexRangeId,
                appendOnlyFile,
                maxLeaps,
                entriesBetweenLeaps,
                rawhide,
                MemoryRawEntryFormat.SINGLETON,
                NoOpFormatTransformerProvider.NO_OP,
                TestUtils.indexType,
                0.75d);

            write.append((stream) -> {
                for (int i = 0; i < counts[ci]; i++) {
                    long time = timeProvider.incrementAndGet();
                    byte[] rawEntry = TestUtils.rawEntry(id.incrementAndGet(), time);
                    if (!stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, new BolBuffer(rawEntry))) {
                        break;
                    }
                }
                return true;
            }, keyBuffer);
            write.closeAppendable(true);

            ReadOnlyFile readOnlyFile = new ReadOnlyFile(file);
            LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
            indexs.append(new ReadOnlyIndex(destroy, indexRangeId, readOnlyFile, NoOpFormatTransformerProvider.NO_OP, rawhide, leapsCache));
        }

        for (int i = 1; i <= id.get(); i++) {
            long g = i;
            byte[] k = UIO.longBytes(i, new byte[8], 0);
            boolean[] passed = { false };
            //System.out.println("Get:" + i);
            indexs.tx(-1, null, null, (index, fromKey, toKey, readIndexs, hydrateValues) -> {

                PointInterleave pointInterleave = new PointInterleave(readIndexs, k, rawhide, true);
                pointInterleave.next((readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                        //System.out.println("\t\tGot:" + UIO.bytesLong(rawEntry.copy(), 4));
                        if (UIO.bytesLong(rawEntry.copy(), 4) == g) {
                            passed[0] = true;
                        }
                        return true;
                    }
                );

                /*for (ReadIndex raw : readIndexs) {
                    System.out.println("\tIndex:" + raw);
                    Scanner scanner = raw.pointScan(new ActiveScan(true), k, new BolBuffer(), new BolBuffer());
                    if (scanner != null) {
                        scanner.next((readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                                System.out.println("\t\tGot:" + UIO.bytesLong(rawEntry.copy(), 4));
                                if (UIO.bytesLong(rawEntry.copy(), 4) == g) {
                                    passed[0] = true;
                                }
                                return true;
                            }
                        );
                    }
                }*/
                return true;
            }, true);
            if (!passed[0]) {
                Assert.fail();
            }
        }

    }

    @Test(enabled = false)
    public void testConcurrentMerges() throws Exception {

        ExecutorService destroy = Executors.newSingleThreadExecutor();
        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(rawhide.getKeyComparator());

        int count = 3;
        int step = 100;
        int indexes = 40;
        boolean fsync = true;
        int minimumRun = 4;

        CompactableIndexes indexs = new CompactableIndexes(new LABStats(), rawhide);
        long time = System.currentTimeMillis();
        System.out.println("Seed:" + time);
        Random rand = new Random(1446914103456L);
        BolBuffer keyBuffer = new BolBuffer();
        Executors.newSingleThreadExecutor().submit(() -> {
            for (int wi = 0; wi < indexes; wi++) {

                File file = File.createTempFile("a-index-" + wi, ".tmp");
                AppendOnlyFile appendOnlyFile = new AppendOnlyFile(file);
                IndexRangeId indexRangeId = new IndexRangeId(wi, wi, 0);

                LABAppendableIndex write = new LABAppendableIndex(new LongAdder(),
                    indexRangeId,
                    appendOnlyFile,
                    64,
                    2,
                    rawhide,
                    new RawEntryFormat(0, 0),
                    NoOpFormatTransformerProvider.NO_OP,
                    TestUtils.indexType, 0.75d);
                TestUtils.append(rand, write, 0, step, count, desired, keyBuffer);
                write.closeAppendable(fsync);

                ReadOnlyFile readOnlyFile = new ReadOnlyFile(file);
                LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
                indexs.append(new ReadOnlyIndex(destroy, indexRangeId, readOnlyFile, NoOpFormatTransformerProvider.NO_OP, rawhide, leapsCache));

            }
            Thread.sleep(10);
            return null;
        });

        assertions(indexs, count, step, desired);

        File indexFiler = File.createTempFile("a-index-merged", ".tmp");
        Callable<Void> compactor = indexs.compactor(new LABStats(), "test", -1, -1, -1, null, minimumRun, fsync,
            (rawhideName, minimumRun1, fsync1, callback) -> callback.call(minimumRun,
                fsync1,
                (id, worstCaseCount) -> {
                    int updatesBetweenLeaps = 2;
                    int maxLeaps = RangeStripedCompactableIndexes.calculateIdealMaxLeaps(worstCaseCount, updatesBetweenLeaps);
                    return new LABAppendableIndex(new LongAdder(),
                        id,
                        new AppendOnlyFile(indexFiler),
                        maxLeaps,
                        updatesBetweenLeaps,
                        rawhide,
                        new RawEntryFormat(0, 0),
                        NoOpFormatTransformerProvider.NO_OP,
                        TestUtils.indexType,
                        0.75d);
                },
                (ids) -> {
                    LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
                    return new ReadOnlyIndex(destroy, ids.get(0), new ReadOnlyFile(indexFiler), NoOpFormatTransformerProvider.NO_OP, rawhide,
                        leapsCache);
                }));

        if (compactor != null) {
            compactor.call();
        }

        assertions(indexs, count, step, desired);
    }

    @Test(enabled = true)
    public void testTx() throws Exception {

        ExecutorService destroy = Executors.newSingleThreadExecutor();
        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(rawhide.getKeyComparator());

        int count = 1;
        int step = 1;
        int indexes = 4;
        boolean fsync = true;
        int minimumRun = 4;

        CompactableIndexes indexs = new CompactableIndexes(new LABStats(), rawhide);
        long time = System.currentTimeMillis();
        System.out.println("Seed:" + time);
        Random rand = new Random(1446914103456L);
        BolBuffer keyBuffer = new BolBuffer();
        for (int wi = 0; wi < indexes; wi++) {

            File file = File.createTempFile("MergableIndexsNGTest" + File.separator
                + "MergableIndexsNGTest-testTx" + File.separator
                + "a-index-" + wi, ".tmp");
            AppendOnlyFile appendOnlyFile = new AppendOnlyFile(file);
            IndexRangeId indexRangeId = new IndexRangeId(wi, wi, 0);

            LABAppendableIndex write = new LABAppendableIndex(new LongAdder(),
                indexRangeId,
                appendOnlyFile,
                64,
                2,
                rawhide,
                new RawEntryFormat(0, 0),
                NoOpFormatTransformerProvider.NO_OP,
                TestUtils.indexType,
                0.75d);
            TestUtils.append(rand, write, 0, step, count, desired, keyBuffer);
            write.closeAppendable(fsync);

            ReadOnlyFile readOnlyFile = new ReadOnlyFile(file);
            LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
            ReadOnlyIndex readOnlyIndex = new ReadOnlyIndex(destroy, indexRangeId, readOnlyFile, NoOpFormatTransformerProvider.NO_OP, rawhide, leapsCache);
            ReadIndex readIndex = readOnlyIndex.acquireReader();
            Scanner scanner = readIndex.rowScan(new ActiveScan(false), new BolBuffer(), new BolBuffer());
            RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                //System.out.println(" Dump:" + TestUtils.toString(rawEntry));
                return true;
            };
            while (scanner.next(stream) == Next.more) {
            }
            readIndex.release();
            indexs.append(readOnlyIndex);
        }

        indexs.tx(-1, null, null, (index1, fromKey, toKey, readIndexs, hydrateValues) -> {
            for (ReadIndex readIndex : readIndexs) {
                //System.out.println("---------------------");
                Scanner rowScan = readIndex.rowScan(new ActiveScan(false), new BolBuffer(), new BolBuffer());
                RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                    //System.out.println(" Found:" + TestUtils.toString(rawEntry));
                    return true;
                };
                while (rowScan.next(stream) == Next.more) {
                }
            }
            return true;
        }, true);

        assertions(indexs, count, step, desired);

        File indexFiler = File.createTempFile("a-index-merged", ".tmp");

        Callable<Void> compactor = indexs.compactor(new LABStats(), "test", -1, -1, -1, null, minimumRun, fsync,
            (rawhideName, minimumRun1, fsync1, callback) -> callback.call(minimumRun1,
                fsync1, (id, worstCaseCount) -> {
                    int updatesBetweenLeaps = 2;
                    int maxLeaps = RangeStripedCompactableIndexes.calculateIdealMaxLeaps(worstCaseCount, updatesBetweenLeaps);
                    return new LABAppendableIndex(new LongAdder(),
                        id,
                        new AppendOnlyFile(indexFiler),
                        maxLeaps,
                        updatesBetweenLeaps,
                        rawhide,
                        new RawEntryFormat(0, 0),
                        NoOpFormatTransformerProvider.NO_OP,
                        TestUtils.indexType,
                        0.75d);
                }, (ids) -> {
                    LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
                    return new ReadOnlyIndex(destroy, ids.get(0), new ReadOnlyFile(indexFiler), NoOpFormatTransformerProvider.NO_OP, rawhide,
                        leapsCache);
                }));

        if (compactor != null) {
            compactor.call();
        } else {
            Assert.fail();
        }

        indexs.tx(-1, null, null, (index1, fromKey, toKey, readIndexs, hydrateValues) -> {
            for (ReadIndex readIndex : readIndexs) {
                //System.out.println("---------------------");
                Scanner rowScan = readIndex.rowScan(new ActiveScan(false), new BolBuffer(), new BolBuffer());
                RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                    //System.out.println(" Found:" + TestUtils.toString(rawEntry));
                    return true;
                };
                while (rowScan.next(stream) == Next.more) {
                }
            }
            return true;
        }, true);

        indexs = new CompactableIndexes(new LABStats(), rawhide);
        IndexRangeId indexRangeId = new IndexRangeId(0, 0, 0);
        ReadOnlyFile indexFile = new ReadOnlyFile(indexFiler);
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        indexs.append(new ReadOnlyIndex(destroy, indexRangeId, indexFile, NoOpFormatTransformerProvider.NO_OP, rawhide, leapsCache));

        assertions(indexs, count, step, desired);
    }

    private void assertions(CompactableIndexes indexs,
        int count, int step,
        ConcurrentSkipListMap<byte[], byte[]> desired) throws
        Exception {

        ArrayList<byte[]> keys = new ArrayList<>(desired.navigableKeySet());

        int[] index = new int[1];
        indexs.tx(-1, null, null, (index1, fromKey, toKey, acquired, hydrateValues) -> {
            AtomicBoolean failed = new AtomicBoolean();
            Scanner rowScan = new InterleaveStream(acquired, null, null, rawhide);
            try {
                RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                    //System.out.println("Expected:key:" + UIO.bytesLong(keys.get(index[0])) + " Found:" + TestUtils.toString(rawEntry));
                    //Assert.assertEquals(UIO.bytesLong(keys.get(index[0])), SimpleRawEntry.key(rawEntry));
                    if (UIO.bytesLong(keys.get(index[0])) != TestUtils.key(rawEntry)) {
                        failed.set(true);
                    }
                    index[0]++;
                    return true;
                };
                while (rowScan.next(stream) == Next.more) {
                }
            } finally {
                rowScan.close();
            }
            Assert.assertFalse(failed.get());

            Assert.assertEquals(index[0], keys.size());
            //System.out.println("rowScan PASSED");
            return true;
        }, true);

        indexs.tx(-1, null, null, (index1, fromKey, toKey, acquired, hydrateValues) -> {
            for (int i = 0; i < count * step; i++) {
                long k = i;
                byte[] key = UIO.longBytes(k, new byte[8], 0);
                PointInterleave pointInterleave = new PointInterleave(acquired, key, rawhide, true);

                RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                    //System.out.println("->" + TestUtils.key(rawEntry) + " " + TestUtils.value(rawEntry));
                    if (rawEntry != null) {
                        //System.out.println("Got: " + TestUtils.toString(rawEntry));
                        byte[] rawKey = UIO.longBytes(TestUtils.key(rawEntry), new byte[8], 0);
                        Assert.assertEquals(rawKey, key);
                        byte[] d = desired.get(key);
                        if (d == null) {
                            Assert.fail();
                        } else {
                            Assert.assertEquals(TestUtils.value(rawEntry.copy()), TestUtils.value(d));
                        }
                    } else {
                        Assert.assertFalse(desired.containsKey(key), "Desired doesn't contain:" + UIO.bytesLong(key));
                    }
                    return rawEntry != null;
                };
                pointInterleave.next(stream);

            }
            //System.out.println("gets PASSED");
            return true;
        }, true);

        indexs.tx(-1, null, null, (index1, fromKey, toKey, acquired, hydrateValues) -> {
            for (int i = 0; i < keys.size() - 3; i++) {
                int _i = i;

                int[] streamed = new int[1];
                RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                    if (TestUtils.value(rawEntry.copy()) > -1) {
                        //System.out.println("Streamed:" + TestUtils.toString(rawEntry));
                        streamed[0]++;
                    }
                    return true;
                };

                //System.out.println("Asked index:" + _i + " key:" + UIO.bytesLong(keys.get(_i)) + " to:" + UIO.bytesLong(keys.get(_i + 3)));
                Scanner rangeScan = new InterleaveStream(acquired, keys.get(_i), keys.get(_i + 3), rawhide);
                try {
                    while (rangeScan.next(stream) == Next.more) {
                    }
                } finally {
                    rangeScan.close();
                }
                Assert.assertEquals(3, streamed[0]);

            }
            //System.out.println("rangeScan PASSED");
            return true;
        }, true);

        indexs.tx(-1, null, null, (index1, fromKey, toKey, acquired, hydrateValues) -> {
            for (int i = 0; i < keys.size() - 3; i++) {
                int _i = i;
                int[] streamed = new int[1];
                RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                    if (TestUtils.value(rawEntry.copy()) > -1) {
                        streamed[0]++;
                    }
                    return true;
                };
                Scanner rangeScan = new InterleaveStream(acquired, UIO.longBytes(UIO.bytesLong(keys.get(_i)) + 1, new byte[8], 0), keys.get(_i + 3),
                    rawhide);
                try {
                    while (rangeScan.next(stream) == Next.more) {
                    }
                } finally {
                    rangeScan.close();
                }
                Assert.assertEquals(2, streamed[0]);

            }
            //System.out.println("rangeScan2 PASSED");
            return true;
        }, true);
    }

}
