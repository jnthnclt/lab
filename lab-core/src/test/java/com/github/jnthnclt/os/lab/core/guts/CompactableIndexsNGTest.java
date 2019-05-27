package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.base.BolBuffer;
import com.github.jnthnclt.os.lab.base.UIO;
import com.github.jnthnclt.os.lab.collections.bah.LRUConcurrentBAHLinkedHash;
import com.github.jnthnclt.os.lab.core.LABEnvironment;
import com.github.jnthnclt.os.lab.core.LABStats;
import com.github.jnthnclt.os.lab.core.TestUtils;
import com.github.jnthnclt.os.lab.core.api.rawhide.LABRawhide;
import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.guts.api.ReadIndex;
import com.github.jnthnclt.os.lab.core.guts.api.Scanner;
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
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class CompactableIndexsNGTest {

    private final Rawhide rawhide = LABRawhide.SINGLETON;

    @DataProvider(name = "indexTypes")
    public static Object[][] indexTypes() {
        return new Object[][] {
            //{ LABHashIndexType.cuckoo },
            { LABHashIndexType.fibCuckoo }
            //, { LABHashIndexType.linearProbe }
        };

    }

    @Test(dataProvider = "indexTypes", enabled = true)
    public void testPointGets(LABHashIndexType hashIndexType) throws Exception {

        ExecutorService destroy = Executors.newSingleThreadExecutor();
        CompactableIndexes indexs = new CompactableIndexes(new LABStats(new AtomicLong()), rawhide, null,null);
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
                hashIndexType,
                0.75d,
                () -> 0);

            write.append((stream) -> {
                for (int i = 0; i < counts[ci]; i++) {
                    long time = timeProvider.incrementAndGet();
                    byte[] rawEntry = TestUtils.rawEntry(id.incrementAndGet(), time);
                    if (!stream.stream(new BolBuffer(rawEntry))) {
                        break;
                    }
                }
                return true;
            }, keyBuffer);
            write.closeAppendable(true);

            ReadOnlyFile readOnlyFile = new ReadOnlyFile(file);
            LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
            indexs.append(new ReadOnlyIndex(null,null, destroy, indexRangeId, readOnlyFile, rawhide, leapsCache));
        }

        for (int i = 1; i <= id.get(); i++) {
            long g = i;
            byte[] k = UIO.longBytes(i, new byte[8], 0);
            boolean[] passed = { false };
            System.out.println(hashIndexType + " Get:" + i);
            indexs.tx(-1, false, null, null,
                (index, pointFrom, fromKey, toKey, readIndexs, hydrateValues) -> {

                    BolBuffer got = PointInterleave.get(readIndexs, k, rawhide, true);
                    if (got != null) {
                        System.out.println(UIO.bytesLong(got.copy(), 4) + " " + g);
                        if (UIO.bytesLong(got.copy(), 4) == g) {
                            passed[0] = true;
                        }
                    }

                    return true;
                }, true);
            if (!passed[0]) {
                Assert.fail();
            }
        }

    }

    @Test(dataProvider = "indexTypes", enabled = false)
    public void testConcurrentMerges(LABHashIndexType hashIndexType) throws Exception {

        ExecutorService destroy = Executors.newSingleThreadExecutor();
        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(rawhide.getKeyComparator());

        int count = 3;
        int step = 100;
        int indexes = 40;
        boolean fsync = true;
        int minimumRun = 4;

        CompactableIndexes indexs = new CompactableIndexes(new LABStats(new AtomicLong()), rawhide, null,null);
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
                    hashIndexType,
                    0.75d,
                    () -> 0);
                TestUtils.append(rand, write, 0, step, count, desired, keyBuffer);
                write.closeAppendable(fsync);

                ReadOnlyFile readOnlyFile = new ReadOnlyFile(file);
                LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
                indexs.append(new ReadOnlyIndex(null,null, destroy, indexRangeId, readOnlyFile, rawhide, leapsCache));

            }
            Thread.sleep(10);
            return null;
        });

        assertions(indexs, count, step, desired);

        File indexFiler = File.createTempFile("a-index-merged", ".tmp");
        Callable<Void> compactor = indexs.compactor(new LABStats(new AtomicLong()), "test", -1, -1, -1, null, minimumRun, fsync,
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
                        TestUtils.indexType,
                        0.75d,
                        () -> 0);
                },
                (ids) -> {
                    LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
                    return new ReadOnlyIndex(null,null, destroy, ids.get(0), new ReadOnlyFile(indexFiler), rawhide,
                        leapsCache);
                }));

        if (compactor != null) {
            compactor.call();
        }

        assertions(indexs, count, step, desired);
    }

    @Test(dataProvider = "indexTypes", enabled = true)
    public void testTx(LABHashIndexType hashIndexType) throws Exception {

        ExecutorService destroy = Executors.newSingleThreadExecutor();
        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(rawhide.getKeyComparator());

        int count = 1;
        int step = 1;
        int indexes = 4;
        boolean fsync = true;
        int minimumRun = 4;

        CompactableIndexes indexs = new CompactableIndexes(new LABStats(new AtomicLong()), rawhide, null,null);
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
                hashIndexType,
                0.75d,
                () -> 0);
            TestUtils.append(rand, write, 0, step, count, desired, keyBuffer);
            write.closeAppendable(fsync);

            ReadOnlyFile readOnlyFile = new ReadOnlyFile(file);
            LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
            ReadOnlyIndex readOnlyIndex = new ReadOnlyIndex(null,null, destroy, indexRangeId, readOnlyFile, rawhide, leapsCache);
            ReadIndex readIndex = readOnlyIndex.acquireReader();
            Scanner scanner = readIndex.rowScan(new BolBuffer(), new BolBuffer());

            BolBuffer rawEntry = new BolBuffer();
            while ((rawEntry = scanner.next(rawEntry, null)) != null) {
            }
            readIndex.release();
            indexs.append(readOnlyIndex);
        }

        indexs.tx(-1, false, null, null,
            (index1, pointFrom1, fromKey, toKey, readIndexs, hydrateValues) -> {

                for (ReadIndex readIndex : readIndexs) {
                    Scanner rowScan = readIndex.rowScan(new BolBuffer(), new BolBuffer());
                    BolBuffer rawEntry = new BolBuffer();
                    while ((rawEntry = rowScan.next(rawEntry, null)) != null) {

                    }
                }
                return true;
            }, true);

        assertions(indexs, count, step, desired);

        File indexFiler = File.createTempFile("a-index-merged", ".tmp");

        Callable<Void> compactor = indexs.compactor(new LABStats(new AtomicLong()), "test", -1, -1, -1, null, minimumRun, fsync,
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
                        TestUtils.indexType,
                        0.75d,
                        () -> 0);
                }, (ids) -> {
                    LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
                    return new ReadOnlyIndex(null,null, destroy, ids.get(0), new ReadOnlyFile(indexFiler), rawhide,
                        leapsCache);
                }));

        if (compactor != null) {
            compactor.call();
        } else {
            Assert.fail();
        }

        indexs.tx(-1, false, null, null,
            (index1, pointFrom1, fromKey, toKey, readIndexs, hydrateValues) -> {

                for (ReadIndex readIndex : readIndexs) {
                    //System.out.println("---------------------");
                    Scanner rowScan = readIndex.rowScan(new BolBuffer(), new BolBuffer());


                    BolBuffer rawEntry = new BolBuffer();
                    while ((rawEntry = rowScan.next(rawEntry, null)) != null) {

                    }
                }
                return true;
            }, true);

        indexs = new CompactableIndexes(new LABStats(new AtomicLong()), rawhide, null,null);
        IndexRangeId indexRangeId = new IndexRangeId(0, 0, 0);
        ReadOnlyFile indexFile = new ReadOnlyFile(indexFiler);
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        indexs.append(new ReadOnlyIndex(null,null, destroy, indexRangeId, indexFile, rawhide, leapsCache));

        assertions(indexs, count, step, desired);
    }

    private void assertions(CompactableIndexes indexs,
        int count, int step,
        ConcurrentSkipListMap<byte[], byte[]> desired) throws
        Exception {

        ArrayList<byte[]> keys = new ArrayList<>(desired.navigableKeySet());

        int[] index = new int[1];
        indexs.tx(-1, false, null, null,
            (index1, pointFrom1, fromKey, toKey, acquired, hydrateValues) -> {

                AtomicBoolean failed = new AtomicBoolean();
                Scanner rowScan = new InterleaveStream(rawhide,
                    ActiveScan.indexToFeeds(acquired, false, false, null, null, rawhide, null));
                try {
                    BolBuffer rawEntry = new BolBuffer();
                    while ((rawEntry = rowScan.next(rawEntry, null)) != null) {
                        if (UIO.bytesLong(keys.get(index[0])) != TestUtils.key(rawEntry)) {
                            failed.set(true);
                        }
                        index[0]++;
                    }
                } finally {
                    rowScan.close();
                }
                Assert.assertFalse(failed.get());

                Assert.assertEquals(index[0], keys.size());
                //System.out.println("rowScan PASSED");
                return true;
            }, true);

        indexs.tx(-1, false, null, null,
            (index1, pointFrom1, fromKey, toKey, acquired, hydrateValues) -> {

                for (int i = 0; i < count * step; i++) {
                    long k = i;
                    byte[] key = UIO.longBytes(k, new byte[8], 0);

                    BolBuffer rawEntry = PointInterleave.get(acquired, key, rawhide, true);
                    if (rawEntry != null) {

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
                    }


                }
                //System.out.println("gets PASSED");
                return true;
            }, true);

        indexs.tx(-1, false, null, null,
            (index1, pointFrom1, fromKey, toKey, acquired, hydrateValues) -> {
                for (int i = 0; i < keys.size() - 3; i++) {
                    int _i = i;

                    int[] streamed = new int[1];


                    //System.out.println("Asked index:" + _i + " key:" + UIO.bytesLong(keys.get(_i)) + " to:" + UIO.bytesLong(keys.get(_i + 3)));
                    Scanner rangeScan = new InterleaveStream(rawhide,
                        ActiveScan.indexToFeeds(acquired, false, false, keys.get(_i), keys.get(_i + 3), rawhide, null));
                    try {
                        BolBuffer rawEntry = new BolBuffer();
                        while ((rawEntry = rangeScan.next(rawEntry, null)) != null) {
                            if (TestUtils.value(rawEntry.copy()) > -1) {
                                //System.out.println("Streamed:" + TestUtils.toString(rawEntry));
                                streamed[0]++;
                            }
                        }
                    } finally {
                        rangeScan.close();
                    }
                    Assert.assertEquals(3, streamed[0]);

                }
                //System.out.println("rangeScan PASSED");
                return true;
            }, true);

        indexs.tx(-1, false, null, null,
            (index1, pointFrom1, fromKey, toKey, acquired, hydrateValues) -> {
                for (int i = 0; i < keys.size() - 3; i++) {
                    int _i = i;
                    int[] streamed = new int[1];

                    Scanner rangeScan = new InterleaveStream(rawhide,
                        ActiveScan.indexToFeeds(acquired, false, false, UIO.longBytes(UIO.bytesLong(keys.get(_i)) + 1, new byte[8], 0), keys.get(_i + 3),
                            rawhide, null));
                    try {

                        BolBuffer rawEntry = new BolBuffer();
                        while ((rawEntry = rangeScan.next(rawEntry, null)) != null) {
                            if (TestUtils.value(rawEntry.copy()) > -1) {
                                streamed[0]++;
                            }
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
