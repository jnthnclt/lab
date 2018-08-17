package com.github.jnthnclt.os.lab.core;

import com.github.jnthnclt.os.lab.base.BolBuffer;
import com.github.jnthnclt.os.lab.base.IndexUtil;
import com.github.jnthnclt.os.lab.base.UIO;
import com.github.jnthnclt.os.lab.collections.bah.LRUConcurrentBAHLinkedHash;
import com.github.jnthnclt.os.lab.core.api.Keys.KeyStream;
import com.github.jnthnclt.os.lab.core.api.MemoryRawEntryFormat;
import com.github.jnthnclt.os.lab.core.api.ScanKeys;
import com.github.jnthnclt.os.lab.core.api.ValueIndex;
import com.github.jnthnclt.os.lab.core.api.ValueIndexConfig;
import com.github.jnthnclt.os.lab.core.api.rawhide.LABRawhide;
import com.github.jnthnclt.os.lab.core.guts.Leaps;
import com.github.jnthnclt.os.lab.core.guts.StripingBolBufferLocks;
import com.github.jnthnclt.os.lab.core.guts.api.KeyToString;
import com.google.common.io.Files;
import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author jonathan.colt
 */
public class LABNGTest {

    private LABEnvironment buildTmpEnv() throws Exception {
        File root = Files.createTempDir();
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        AtomicLong globalHeapCostInBytes = new AtomicLong();
        LABStats stats = new LABStats(globalHeapCostInBytes);
        LABHeapPressure labHeapPressure = new LABHeapPressure(stats,
            LABEnvironment.buildLABHeapSchedulerThreadPool(1),
            "default",
            1024 * 1024 * 5,
            1024 * 1024 * 10,
            globalHeapCostInBytes,
            LABHeapPressure.FreeHeapStrategy.mostBytesFirst);
        return new LABEnvironment(stats,
            null,
            LABEnvironment.buildLABSchedulerThreadPool(1),
            LABEnvironment.buildLABCompactorThreadPool(4),
            LABEnvironment.buildLABDestroyThreadPool(1),
            null,
            root,
            labHeapPressure, 1, 2, leapsCache,
            new StripingBolBufferLocks(1024),
            true,
            false);
    }

    private ValueIndex buildTmpValueIndex(LABEnvironment env) throws Exception {
        return buildTmpValueIndex(env, Long.MAX_VALUE);
    }

    private ValueIndex buildTmpValueIndex(LABEnvironment env, long deleteTombstonedVersionsAfterMillis) throws Exception {
        long splitAfterSizeInBytes = 16; //1024 * 1024 * 1024;

        ValueIndexConfig valueIndexConfig = new ValueIndexConfig("foo",
            4096,
            1024 * 1024 * 10,
            splitAfterSizeInBytes,
            -1,
            -1,
            "deprecated",
            LABRawhide.NAME,
            MemoryRawEntryFormat.NAME,
            2,
            TestUtils.indexType,
            0.75d,
            false,
            deleteTombstonedVersionsAfterMillis);

        return env.open(valueIndexConfig);
    }

    @Test
    public void testRangeScanInsane() throws Exception {
        boolean fsync = false;
        LABEnvironment env = buildTmpEnv();
        ValueIndex index = buildTmpValueIndex(env);

        AtomicLong count = new AtomicLong();
        AtomicLong fails = new AtomicLong();

        BolBuffer rawEntryBuffer = new BolBuffer();
        BolBuffer keyBuffer = new BolBuffer();
        while (count.get() < 100) {
            index.append((stream) -> {
                for (int i = 0; i < 10; i++) {
                    long v = count.get();
                    stream.stream(-1, UIO.longBytes(v), v, false, 0, UIO.longBytes(v));
                    count.incrementAndGet();
                }
                return true;
            }, fsync, rawEntryBuffer, keyBuffer);
            //System.out.println("Pre Commit");
            long c = count.get();
            AtomicLong f;
            do {
                f = new AtomicLong();
                assertRangeScan(c, index, f);
                if (f.get() > 0) {
                    //System.out.println("SPINNING ");
                }
                fails.addAndGet(f.get());
            }
            while (f.get() > 0);
            index.commit(true, true);
            //System.out.println("Post Commit");
            do {
                f = new AtomicLong();
                assertRangeScan(c, index, f);
                if (f.get() > 0) {
                    //System.out.println("SPINNING");
                }
                fails.addAndGet(f.get());
            }
            while (f.get() > 0);
            //System.out.println(c + " -------------------------------------");
        }

        //System.out.println("fails:" + fails.get());
        assertEquals(fails.get(), 0);
        env.shutdown();
    }

    private void assertRangeScan(long c, ValueIndex index, AtomicLong fails) throws Exception {

        for (long f = 0; f < c; f++) {

            for (long t = f; t < c; t++) {

                long ff = f;
                long tt = t;

                //System.out.println("scan:" + ff + " -> " + tt);
                HashSet<Long> rangeScan = new HashSet<>();
                index.rangeScan(UIO.longBytes(f), UIO.longBytes(t),
                    (index1, key, timestamp, tombstoned, version, payload) -> {
                        long got = key.getLong(0);
                        boolean added = rangeScan.add(got);
                        //Assert.assertTrue(scanned.add(UIO.bytesLong(key)), "Already contained " + UIO.bytesLong(key));
                        if (!added) {
                            fails.incrementAndGet();
                            ((LAB) index).auditRanges(new KeyToString() {
                                @Override
                                public String keyToString(byte[] key) {
                                    return "" + UIO.bytesLong(key);
                                }
                            });
                            //System.out.println("RANGE FAILED: from:" + ff + " to:" + tt + " already contained " + got);
                            //System.out.println();
                        }
                        return true;
                    }, true);

                if (rangeScan.size() != t - f) {
                    fails.incrementAndGet();
                    ((LAB) index).auditRanges(new KeyToString() {
                        @Override
                        public String keyToString(byte[] key) {
                            return "" + UIO.bytesLong(key);
                        }
                    });
                    //System.out.print("RANGE FAILED: from:" + f + " to:" + t + " result:" + rangeScan);
                    //System.out.println();
                }
            }

        }

        HashSet<Long> rowScan = new HashSet<>();
        index.rowScan((index1, key, timestamp, tombstoned, version, payload) -> {
            boolean added = rowScan.add(key.getLong(0));
            //Assert.assertTrue(scanned.add(UIO.bytesLong(key)), "Already contained " + UIO.bytesLong(key));
            if (!added) {
                fails.incrementAndGet();
                long got = key.getLong(0);
                //System.out.println("RANGE FAILED: already contained " + got);
            }
            return true;
        }, true);

        if (rowScan.size() != c) {
            fails.incrementAndGet();
            //System.out.print("ROW FAILED: expected " + c + " result:" + rowScan);
        }
    }

    @Test
    public void testRowScan() throws Exception {

        boolean fsync = false;
        LABEnvironment env = buildTmpEnv();
        ValueIndex index = buildTmpValueIndex(env);

        BolBuffer rawEntryBuffer = new BolBuffer();
        BolBuffer keyBuffer = new BolBuffer();
        index.append((stream) -> {
            stream.stream(-1, UIO.longBytes(1), System.currentTimeMillis(), false, 0, UIO.longBytes(1));
            stream.stream(-1, UIO.longBytes(2), System.currentTimeMillis(), false, 0, UIO.longBytes(2));
            stream.stream(-1, UIO.longBytes(3), System.currentTimeMillis(), false, 0, UIO.longBytes(3));
            return true;
        }, fsync, rawEntryBuffer, keyBuffer);
        commitAndWait(index, fsync);

        Assert.assertFalse(index.isEmpty());
        long[] expected = new long[] { 1, 2, 3 };
        testScanExpected(index, expected);
        env.shutdown();

    }

    @Test
    public void testPointRangeScan() throws Exception {

        boolean fsync = false;
        LABEnvironment env = buildTmpEnv();
        ValueIndex index = buildTmpValueIndex(env);

        BolBuffer rawEntryBuffer = new BolBuffer();
        BolBuffer keyBuffer = new BolBuffer();
        index.append((stream) -> {
            long timestamp = System.currentTimeMillis();
            stream.stream(-1, UIO.longBytes(10), timestamp, false, 0, UIO.longBytes(10));
            stream.stream(-1, UIO.longBytes(30), timestamp, false, 0, UIO.longBytes(30));
            stream.stream(-1, UIO.longBytes(50), timestamp, false, 0, UIO.longBytes(50));
            stream.stream(-1, UIO.longBytes(70), timestamp, false, 0, UIO.longBytes(70));
            stream.stream(-1, UIO.longBytes(90), timestamp, false, 0, UIO.longBytes(90));
            return true;
        }, fsync, rawEntryBuffer, keyBuffer);
        commitAndWait(index, fsync);

        testPointRangeScanExpected(index, UIO.longBytes(9), UIO.longBytes(10), new long[] {});
        testPointRangeScanExpected(index, UIO.longBytes(10), UIO.longBytes(10), new long[] {});
        testPointRangeScanExpected(index, UIO.longBytes(10), UIO.longBytes(11), new long[] { 10 });
        testPointRangeScanExpected(index, UIO.longBytes(10), UIO.longBytes(30), new long[] { 10 });
        testPointRangeScanExpected(index, UIO.longBytes(10), UIO.longBytes(31), new long[] { 10, 30 });
        testPointRangeScanExpected(index, UIO.longBytes(10), UIO.longBytes(71), new long[] { 10, 30, 50, 70 });
        testPointRangeScanExpected(index, UIO.longBytes(70), null, new long[] { 70, 90 });
        testPointRangeScanExpected(index, UIO.longBytes(69), null, new long[] {});
        testPointRangeScanExpected(index, UIO.longBytes(91), null, new long[] {});

        env.shutdown();

    }

    @Test
    public void testPointRangeScan2() throws Exception {

        boolean fsync = false;
        LABEnvironment env = buildTmpEnv();
        ValueIndex index = buildTmpValueIndex(env);

        BolBuffer rawEntryBuffer = new BolBuffer();
        BolBuffer keyBuffer = new BolBuffer();
        index.append((stream) -> {
            long timestamp = System.currentTimeMillis();
            stream.stream(-1, UIO.longBytes(10), timestamp, false, 0, UIO.longBytes(10));
            return true;
        }, fsync, rawEntryBuffer, keyBuffer);
        commitAndWait(index, fsync);

        testPointRangeScanExpected(index, UIO.longBytes(9), UIO.longBytes(10), new long[] {});
        testPointRangeScanExpected(index, UIO.longBytes(10), UIO.longBytes(10), new long[] {});
        testPointRangeScanExpected(index, UIO.longBytes(10), UIO.longBytes(11), new long[] { 10 });
        env.shutdown();

    }

    @Test
    public void testSkipRowScan() throws Exception {

        boolean fsync = false;
        LABEnvironment env = buildTmpEnv();
        ValueIndex index = buildTmpValueIndex(env);

        BolBuffer rawEntryBuffer = new BolBuffer();
        BolBuffer keyBuffer = new BolBuffer();
        index.append((stream) -> {
            long timestamp = System.currentTimeMillis();
            stream.stream(-1, UIO.longBytes(10), timestamp, false, 0, UIO.longBytes(10));
            stream.stream(-1, UIO.longBytes(30), timestamp, false, 0, UIO.longBytes(30));
            stream.stream(-1, UIO.longBytes(50), timestamp, false, 0, UIO.longBytes(50));
            stream.stream(-1, UIO.longBytes(70), timestamp, false, 0, UIO.longBytes(70));
            stream.stream(-1, UIO.longBytes(90), timestamp, false, 0, UIO.longBytes(90));
            return true;
        }, fsync, rawEntryBuffer, keyBuffer);
        commitAndWait(index, fsync);

        Assert.assertFalse(index.isEmpty());
        testScanExpected(index, new long[] { 10, 30, 50, 70, 90 });

        testSkipScanExpected(index, new long[] { 0, 1, 2 },
            new long[] { -1, -1, -1 });

        testSkipScanExpected(index, new long[] { 31, 32, 33, 71, 73, 75 },
            new long[] { -1, -1, -1, -1, -1, -1 });

        testSkipScanExpected(index, new long[] { 10, 30, 50, 70, 90 },
            new long[] { 10, 30, 50, 70, 90 });

        testSkipScanExpected(index, new long[] { 0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 },
            new long[] { -1, 10, -1, 30, -1, 50, -1, 70, -1, 90, -1 });

        testSkipScanExpected(index, new long[] { 90, 92, 93 },
            new long[] { 90, -1, -1 });

        testSkipScanExpected(index, new long[] { 91, 92, 93 },
            new long[] { -1, -1, -1 });


        env.shutdown();

    }

    private void testSkipScanExpected(ValueIndex index, long[] desired, long[] expected) throws Exception {

        System.out.println("Checking skip scan");
        List<Long> scanned = new ArrayList<>();

        ScanKeys scanKeys = new ScanKeys() {
            int i = 0;

            @Override
            public BolBuffer nextKey() throws Exception {
                if (i < desired.length) {
                    try {
                        return new BolBuffer(UIO.longBytes(desired[i]));
                    } finally {
                        i += 1;
                    }
                }
                return null;
            }
        };


        index.rowScan(scanKeys,
            (index1, key, timestamp, tombstoned, version, payload) -> {
                if (payload != null && !tombstoned) {
                    System.out.println(
                        "scan:" + IndexUtil.toString(key) + " " + timestamp + " " + tombstoned + " " + version + " " + IndexUtil.toString(payload));
                    scanned.add(payload.getLong(0));
                } else {
                    scanned.add(-1L);
                }
                return true;
            }, true);

        assertEquals(scanned.size(), expected.length);

        for (int i = 0; i < expected.length; i++) {
            System.out.println(scanned.get(i) + " vs " + expected[i]);
            assertEquals((long) scanned.get(i), expected[i]);
        }
    }


    @Test
    public void testEnv() throws Exception {

        boolean fsync = false;
        LABEnvironment env = buildTmpEnv();
        ValueIndex index = buildTmpValueIndex(env);

        BolBuffer rawEntryBuffer = new BolBuffer();
        BolBuffer keyBuffer = new BolBuffer();
        index.append((stream) -> {
            stream.stream(-1, UIO.longBytes(1), System.currentTimeMillis(), false, 0, UIO.longBytes(1));
            stream.stream(-1, UIO.longBytes(2), System.currentTimeMillis(), false, 0, UIO.longBytes(2));
            stream.stream(-1, UIO.longBytes(3), System.currentTimeMillis(), false, 0, UIO.longBytes(3));
            return true;
        }, fsync, rawEntryBuffer, keyBuffer);
        commitAndWait(index, fsync);

        index.append((stream) -> {
            stream.stream(-1, UIO.longBytes(7), System.currentTimeMillis(), false, 0, UIO.longBytes(7));
            stream.stream(-1, UIO.longBytes(8), System.currentTimeMillis(), false, 0, UIO.longBytes(8));
            stream.stream(-1, UIO.longBytes(9), System.currentTimeMillis(), false, 0, UIO.longBytes(9));
            return true;
        }, fsync, rawEntryBuffer, keyBuffer);
        commitAndWait(index, fsync);

        Assert.assertFalse(index.isEmpty());

        index.rowScan((index1, key, timestamp, tombstoned, version, payload) -> {
            //System.out.println(Arrays.toString(key) + " " + timestamp + " " + tombstoned + " " + version + " " + Arrays.toString(payload));
            return true;
        }, true);

        long[] expected = new long[] { 1, 2, 3, 7, 8, 9 };
        testExpected(index, expected);
        testExpectedMultiGet(index, expected);
        testNotExpected(index, new long[] { 0, 4, 5, 6, 10 });
        testNotExpectedMultiGet(index, new long[] { 0, 4, 5, 6, 10 });
        testScanExpected(index, expected);
        testRangeScanExpected(index, UIO.longBytes(2), null, new long[] { 2, 3, 7, 8, 9 });
        testRangeScanExpected(index, UIO.longBytes(2), UIO.longBytes(7), new long[] { 2, 3 });
        testRangeScanExpected(index, UIO.longBytes(4), UIO.longBytes(7), new long[] {});

        index.commit(fsync, true);

        index.append((stream) -> {
            stream.stream(-1, UIO.longBytes(1), System.currentTimeMillis(), true, 1, UIO.longBytes(1));
            stream.stream(-1, UIO.longBytes(2), System.currentTimeMillis(), true, 1, UIO.longBytes(2));
            stream.stream(-1, UIO.longBytes(3), System.currentTimeMillis(), true, 1, UIO.longBytes(3));
            return true;
        }, fsync, rawEntryBuffer, keyBuffer);

        expected = new long[] { 7, 8, 9 };
        testExpected(index, expected);
        testExpectedMultiGet(index, expected);
        testNotExpected(index, new long[] { 0, 4, 5, 6, 10 });
        testNotExpectedMultiGet(index, new long[] { 0, 4, 5, 6, 10 });
        testScanExpected(index, expected);
        testRangeScanExpected(index, UIO.longBytes(1), UIO.longBytes(9), new long[] { 7, 8 });

        env.shutdown();

    }

    @Test
    public void testClobber() throws Exception {

        boolean fsync = false;
        LABEnvironment env = buildTmpEnv();
        ValueIndex index = buildTmpValueIndex(env);

        BolBuffer rawEntryBuffer = new BolBuffer();
        BolBuffer keyBuffer = new BolBuffer();

        index.append((stream) -> {
            stream.stream(-1, UIO.longBytes(1), 1, false, 0, UIO.longBytes(1));
            return true;
        }, fsync, rawEntryBuffer, keyBuffer);

        commitAndWait(index, fsync);

        index.append((stream) -> {
            stream.stream(-1, UIO.longBytes(1), 4, false, 0, UIO.longBytes(7));
            return true;
        }, fsync, rawEntryBuffer, keyBuffer);
        commitAndWait(index, fsync);

        index.append((stream) -> {
            stream.stream(-1, UIO.longBytes(1), 1, false, 0, UIO.longBytes(1));
            return true;
        }, fsync, rawEntryBuffer, keyBuffer);

        commitAndWait(index, fsync);

        Assert.assertFalse(index.isEmpty());

        long[] expectedValues = new long[] { -1, 7 };

        index.get((keyStream) -> {
            for (int i = 1; i < 2; i++) {
                keyStream.key(i, UIO.longBytes(i), 0, 8);
            }
            return true;
        }, (index1, key, timestamp, tombstoned, version, payload) -> {
            //System.out.println(IndexUtil.toString(key) + " " + timestamp + " " + tombstoned + " " + version + " " + IndexUtil.toString(payload));
            assertEquals(UIO.bytesLong(payload.copy()), expectedValues[index1]);
            return true;
        }, true);

        env.shutdown();

    }

    @Test
    public void testBackwards() throws Exception {

        boolean fsync = false;
        LABEnvironment env = buildTmpEnv();
        ValueIndex index = buildTmpValueIndex(env);


        BolBuffer rawEntryBuffer = new BolBuffer();
        BolBuffer keyBuffer = new BolBuffer();

        byte[] key = UIO.longBytes(1234);
        for (int i = 0; i < 10; i++) {
            long timestamp = Integer.MAX_VALUE - i;
            long version = timestamp + 1;
            index.append((stream) -> {
                //System.out.println("wrote timestamp:" + timestamp + " version:" + version);
                stream.stream(-1, key, timestamp, false, version, UIO.longBytes(timestamp));
                return true;
            }, fsync, rawEntryBuffer, keyBuffer);
        }

        commitAndWait(index, fsync);

        for (int i = 10; i < 20; i++) {
            long timestamp = Integer.MAX_VALUE - i;
            long version = timestamp + 1;
            index.append((stream) -> {
                //System.out.println("wrote timestamp:" + timestamp + " version:" + version);
                stream.stream(-1, key, timestamp, false, version, UIO.longBytes(timestamp));
                return true;
            }, fsync, rawEntryBuffer, keyBuffer);
        }

        long[] gotTimestamp = { -1 };
        index.get((keyStream) -> {
            keyStream.key(0, UIO.longBytes(1234), 0, 8);
            return true;
        }, (index1, key1, timestamp, tombstoned, version, payload) -> {
            gotTimestamp[0] = timestamp;
            //System.out.println(IndexUtil.toString(key1) + " " + timestamp + " " + tombstoned + " " + version + " " + IndexUtil.toString(payload));
            return true;
        }, true);

        env.shutdown();
        assertEquals(gotTimestamp[0], Integer.MAX_VALUE);
    }

    @Test
    public void testKeyValue() throws Exception {

        boolean fsync = false;
        LABEnvironment env = buildTmpEnv();
        ValueIndex index = buildTmpValueIndex(env);


        BolBuffer rawEntryBuffer = new BolBuffer();
        BolBuffer keyBuffer = new BolBuffer();

        byte[] key = UIO.longBytes(1234);
        for (int i = 0; i < 10; i++) {
            long timestamp = i;
            index.append((stream) -> {
                //System.out.println("wrote timestamp:" + timestamp);
                stream.stream(-1, key, 0, false, 0, UIO.longBytes(timestamp));
                return true;
            }, fsync, rawEntryBuffer, keyBuffer);
        }

        commitAndWait(index, fsync);

        for (int i = 10; i < 20; i++) {
            long timestamp = i;
            index.append((stream) -> {
                //System.out.println("wrote timestamp:" + timestamp);
                stream.stream(-1, key, 0, false, 0, UIO.longBytes(timestamp));
                return true;
            }, fsync, rawEntryBuffer, keyBuffer);
        }

        long[] gotTimestamp = { -1 };
        index.get((keyStream) -> {
            keyStream.key(0, UIO.longBytes(1234), 0, 8);
            return true;
        }, (index1, key1, timestamp, tombstoned, version, payload) -> {
            gotTimestamp[0] = payload.getLong(0);
            //System.out.println(IndexUtil.toString(key1) + " " + timestamp + " " + tombstoned + " " + version + " " + IndexUtil.toString(payload));
            return true;
        }, true);

        env.shutdown();
        assertEquals(gotTimestamp[0], 19);
    }

    @Test(enabled = false)
    public void testTombstones() throws Exception {

        boolean fsync = false;
        LABEnvironment env = buildTmpEnv();
        ValueIndex index = buildTmpValueIndex(env, 25);

        BolBuffer rawEntryBuffer = new BolBuffer();
        BolBuffer keyBuffer = new BolBuffer();
        int count = 10;

        index.append((stream) -> {
            for (int i = 0; i < count; i++) {
                stream.stream(i, UIO.longBytes(i), 1, false, System.currentTimeMillis(), UIO.longBytes(i));
            }
            return true;
        }, fsync, rawEntryBuffer, keyBuffer);

        commitAndWait(index, fsync);

        long[] expectedValues = new long[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

        index.get((keyStream) -> {
            for (int i = 0; i < expectedValues.length; i++) {
                keyStream.key(i, UIO.longBytes(i), 0, 8);
            }
            return true;
        }, (index1, key, timestamp, tombstoned, version, payload) -> {
            //System.out.println(IndexUtil.toString(key) + " " + timestamp + " " + tombstoned + " " + version + " " + IndexUtil.toString(payload));
            assertEquals(UIO.bytesLong(payload.copy()), expectedValues[index1]);
            return true;
        }, true);


        index.append((stream) -> {
            for (int i = 0; i < count; i++) {
                stream.stream(i, UIO.longBytes(i), 2, true, 0, UIO.longBytes(i));
            }
            return true;
        }, fsync, rawEntryBuffer, keyBuffer);

        commitAndWait(index, fsync);
        Thread.sleep(1000); // LAME
        index.compact(true, 0, 0, true);


        AtomicLong failures = new AtomicLong();
        index.get((keyStream) -> {
            for (int i = 1; i < count; i++) {
                keyStream.key(i, UIO.longBytes(i), 0, 8);
            }
            return true;
        }, (index1, key, timestamp, tombstoned, version, payload) -> {
            if (payload != null) {
                System.out.println("GRRR:" + IndexUtil.toString(key) + " " + timestamp + " " + tombstoned + " " + version + " " + IndexUtil.toString(payload));
                failures.incrementAndGet();
            }
            return true;
        }, true);
        Assert.assertEquals(failures.get(),0);

        env.shutdown();

    }


    private void commitAndWait(ValueIndex index, boolean fsync) throws Exception, ExecutionException, InterruptedException {
        List<Future<Object>> awaitable = index.commit(fsync, true);
        for (Future<Object> future : awaitable) {
            future.get();
        }
    }

    private void testExpectedMultiGet(ValueIndex index, long[] expected) throws Exception {
        index.get((KeyStream keyStream) -> {
            for (int i = 0; i < expected.length; i++) {
                keyStream.key(i, UIO.longBytes(expected[i]), 0, 8);
            }
            return true;
        }, (index1, key, timestamp, tombstoned, version, payload) -> {
            assertEquals(payload.getLong(0), expected[index1]);
            return true;
        }, true);
    }

    private void testExpected(ValueIndex index, long[] expected) throws Exception {
        for (int i = 0; i < expected.length; i++) {
            long e = expected[i];
            int ii = i;
            index.get(
                (keyStream) -> {
                    byte[] key = UIO.longBytes(expected[ii]);
                    keyStream.key(0, key, 0, key.length);
                    return true;
                },
                (index1, key, timestamp, tombstoned, version, payload) -> {
                    assertEquals(payload.getLong(0), e);
                    return true;
                }, true);
        }
    }

    private void testNotExpectedMultiGet(ValueIndex index, long[] notExpected) throws Exception {
        index.get((KeyStream keyStream) -> {
            for (long i : notExpected) {
                keyStream.key(-1, UIO.longBytes(i), 0, 8);
            }
            return true;
        }, (index1, key, timestamp, tombstoned, version, payload) -> {
            if (key != null || payload != null) {
                Assert.fail(IndexUtil.toString(key) + " " + timestamp + " " + tombstoned + " " + version + " " + IndexUtil.toString(payload));
            }
            return true;
        }, true);
    }

    private void testNotExpected(ValueIndex index, long[] notExpected) throws Exception {
        for (long i : notExpected) {
            long ii = i;
            index.get(
                (keyStream) -> {
                    byte[] key = UIO.longBytes(ii);
                    keyStream.key(0, key, 0, key.length);
                    return true;
                },
                (index1, key, timestamp, tombstoned, version, payload) -> {
                    if (key != null || payload != null) {
                        Assert.fail(IndexUtil.toString(key) + " " + timestamp + " " + tombstoned + " " + version + " " + IndexUtil.toString(payload));
                    }
                    return true;
                }, true);
        }
    }

    private void testScanExpected(ValueIndex index, long[] expected) throws Exception {

        System.out.println("Checking full scan");
        List<Long> scanned = new ArrayList<>();
        index.rowScan((index1, key, timestamp, tombstoned, version, payload) -> {
            if (!tombstoned) {
                System.out.println("scan:" + IndexUtil.toString(key) + " " + timestamp + " " + tombstoned + " " + version + " " + IndexUtil.toString(payload));
                scanned.add(payload.getLong(0));
            }
            return true;
        }, true);

        assertEquals(scanned.size(), expected.length);

        for (int i = 0; i < expected.length; i++) {
            System.out.println((long) scanned.get(i) + " vs " + expected[i]);
            assertEquals((long) scanned.get(i), expected[i]);
        }
    }

    private void testRangeScanExpected(ValueIndex index, byte[] from, byte[] to, long[] expected) throws Exception {

        //System.out.println("Checking range scan:" + Arrays.toString(from) + "->" + Arrays.toString(to));
        List<Long> scanned = new ArrayList<>();
        index.rangeScan(from, to, (index1, key, timestamp, tombstoned, version, payload) -> {
            //System.out.println("scan:" + IndexUtil.toString(key) + " " + timestamp + " " + tombstoned + " " + version + " " + IndexUtil.toString(payload));
            if (!tombstoned) {
                scanned.add(payload.getLong(0));
            }
            return true;
        }, true);
        assertEquals(scanned.size(), expected.length);
        for (int i = 0; i < expected.length; i++) {
            //System.out.println((long) scanned.get(i) + " vs " + expected[i]);
            assertEquals((long) scanned.get(i), expected[i]);
        }
    }

    private void testPointRangeScanExpected(ValueIndex index, byte[] from, byte[] to, long[] expected) throws Exception {

        //System.out.println("Checking point range scan:" + Arrays.toString(from) + "->" + Arrays.toString(to));
        List<Long> scanned = new ArrayList<>();
        index.pointRangeScan(from, to, (index1, key, timestamp, tombstoned, version, payload) -> {
            if (!tombstoned) {
                //System.out.println("scan:" + IndexUtil.toString(key) + " " + timestamp + " " + tombstoned + " " + version + " " + IndexUtil.toString
                // (payload));
                scanned.add(payload.getLong(0));
            }
            return true;
        }, true);


        assertEquals(expected.length, scanned.size(), "size miss match");
        for (int i = 0; i < expected.length; i++) {
            //System.out.println(scanned.get(i) + " vs " + expected[i]);
            assertEquals((long) scanned.get(i), expected[i]);
        }
    }

}
