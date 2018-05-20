package com.github.jnthnclt.os.lab.core.bitmaps;

import com.github.jnthnclt.os.lab.core.LABEnvironment;
import com.github.jnthnclt.os.lab.core.LABHeapPressure;
import com.github.jnthnclt.os.lab.core.LABStats;
import com.github.jnthnclt.os.lab.core.api.MemoryRawEntryFormat;
import com.github.jnthnclt.os.lab.core.api.ValueIndex;
import com.github.jnthnclt.os.lab.core.api.ValueIndexConfig;
import com.github.jnthnclt.os.lab.core.api.rawhide.LABRawhide;
import com.github.jnthnclt.os.lab.core.guts.LABFiles;
import com.github.jnthnclt.os.lab.core.guts.LABHashIndexType;
import com.github.jnthnclt.os.lab.core.guts.StripingBolBufferLocks;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.LABBitmapAndLastId;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class LABBitmapIndexTest {

    @Test(dataProvider = "labInvertedIndexDataProviderWithData",
        groups = "slow", enabled = false, description = "Performance test")
    public void testSetId(LABBitmapIndex<RoaringBitmap, RoaringBitmap> bitmapIndex, int appends, int sets) throws Exception {
        Random r = new Random(1_234);
        int id = 0;
        int[] ids = new int[sets + appends];
        for (int i = 0; i < appends; i++) {
            id += 1 + (r.nextDouble() * 120_000);
            bitmapIndex.set(id);
            ids[i] = id;
            if (i % 100_000 == 0) {
                System.out.println("add " + i);
            }
        }

        System.out.println("max id " + id);
        System.out.println("bitmap size " + getIndex(bitmapIndex).getBitmap().getSizeInBytes());
        //Thread.sleep(2000);

        long timestamp = System.currentTimeMillis();
        long subTimestamp = System.currentTimeMillis();
        for (int i = 0; i < sets; i++) {
            bitmapIndex.remove(ids[i]);

            id += 1 + (r.nextDouble() * 120);
            bitmapIndex.set(id);
            ids[appends + i] = id;

            if (i % 1_000 == 0) {
                //System.out.println("set " + i);
                System.out.println(String.format("set 1000, elapsed = %s, max id = %s, bitmap size = %s",
                    (System.currentTimeMillis() - subTimestamp), id, getIndex(bitmapIndex).getBitmap().getSizeInBytes()));
                subTimestamp = System.currentTimeMillis();
            }
        }
        System.out.println("elapsed: " + (System.currentTimeMillis() - timestamp) + " ms");
    }

    @Test(dataProvider = "labInvertedIndexDataProviderWithData")
    public void testAppend(LABBitmapIndex<RoaringBitmap, RoaringBitmap> bitmapIndex, List<Integer> ids) throws Exception {
        LABBitmaps<RoaringBitmap, RoaringBitmap> bitmaps = new RoaringLABBitmaps();

        int lastId = ids.get(ids.size() - 1);
        bitmapIndex.set(lastId + 1);
        bitmapIndex.set(lastId + 3);

        for (int id : ids) {
            assertTrue(bitmaps.isSet(getIndex(bitmapIndex).getBitmap(), id));
        }

        assertTrue(bitmaps.isSet(getIndex(bitmapIndex).getBitmap(), lastId + 1));
        assertFalse(bitmaps.isSet(getIndex(bitmapIndex).getBitmap(), lastId + 2));
        assertTrue(bitmaps.isSet(getIndex(bitmapIndex).getBitmap(), lastId + 3));
        assertFalse(bitmaps.isSet(getIndex(bitmapIndex).getBitmap(), lastId + 4));
    }

    @Test(dataProvider = "labInvertedIndexDataProviderWithData")
    public void testRemove(LABBitmapIndex<RoaringBitmap, RoaringBitmap> bitmapIndex, List<Integer> ids) throws Exception {
        RoaringBitmap index = getIndex(bitmapIndex).getBitmap();
        assertEquals(index.getCardinality(), ids.size());

        for (int id : ids) {
            assertTrue(index.contains(id));
        }

        for (int i = 0; i < ids.size() / 2; i++) {
            bitmapIndex.remove(ids.get(i));
        }
        ids = ids.subList(ids.size() / 2, ids.size());

        index = getIndex(bitmapIndex).getBitmap();
        assertEquals(index.getCardinality(), ids.size());
        for (int id : ids) {
            assertTrue(index.contains(id));
        }
    }

    @Test(dataProvider = "labInvertedIndexDataProviderWithData")
    public void testSet(LABBitmapIndex<RoaringBitmap, RoaringBitmap> bitmapIndex, List<Integer> ids) throws Exception {
        LABBitmaps<RoaringBitmap, RoaringBitmap> bitmaps = new RoaringLABBitmaps();

        int lastId = ids.get(ids.size() - 1);

        bitmapIndex.set(lastId + 2);
        bitmapIndex.set(lastId + 1);

        for (int id : ids) {
            assertTrue(bitmaps.isSet(getIndex(bitmapIndex).getBitmap(), id));
        }
        assertTrue(bitmaps.isSet(getIndex(bitmapIndex).getBitmap(), lastId + 1));
        assertTrue(bitmaps.isSet(getIndex(bitmapIndex).getBitmap(), lastId + 2));
    }

    @Test(dataProvider = "labInvertedIndexDataProviderWithData")
    public void testSetNonIntermediateBit(LABBitmapIndex<RoaringBitmap, RoaringBitmap> bitmapIndex,
        List<Integer> ids) throws Exception {
        LABBitmaps<RoaringBitmap, RoaringBitmap> bitmaps = new RoaringLABBitmaps();

        int lastId = ids.get(ids.size() - 1);

        bitmapIndex.set(lastId + 1);
        bitmapIndex.set(lastId + 2);

        for (int id : ids) {
            assertTrue(bitmaps.isSet(getIndex(bitmapIndex).getBitmap(), id));
        }
        assertTrue(bitmaps.isSet(getIndex(bitmapIndex).getBitmap(), lastId + 1));
        assertTrue(bitmaps.isSet(getIndex(bitmapIndex).getBitmap(), lastId + 2));
    }

    @Test(dataProvider = "labInvertedIndexDataProviderWithData")
    public void testAndNot(LABBitmapIndex<RoaringBitmap, RoaringBitmap> bitmapIndex, List<Integer> ids) throws Exception {
        LABBitmaps<RoaringBitmap, RoaringBitmap> bitmaps = new RoaringLABBitmaps();

        RoaringBitmap bitmap = new RoaringBitmap();
        for (int i = 0; i < ids.size() / 2; i++) {
            bitmap.add(ids.get(i));
        }
        bitmapIndex.andNot(bitmap);

        for (int i = 0; i < ids.size() / 2; i++) {
            RoaringBitmap got = getIndex(bitmapIndex).getBitmap();
            assertFalse(bitmaps.isSet(got, ids.get(i)), "Mismatch at " + i);
        }
        for (int i = ids.size() / 2; i < ids.size(); i++) {
            RoaringBitmap got = getIndex(bitmapIndex).getBitmap();
            assertTrue(bitmaps.isSet(got, ids.get(i)), "Mismatch at " + i);
        }
    }

    @Test(dataProvider = "labInvertedIndexDataProviderWithData")
    public void testAndNotToSourceSize(LABBitmapIndex<RoaringBitmap, RoaringBitmap> bitmapIndex, List<Integer> ids) throws Exception {
        LABBitmaps<RoaringBitmap, RoaringBitmap> bitmaps = new RoaringLABBitmaps();

        RoaringBitmap bitmap = new RoaringBitmap();
        for (int i = 0; i < ids.size() / 2; i++) {
            bitmap.add(ids.get(i));
        }
        bitmapIndex.andNotToSourceSize(Collections.singletonList(bitmap));

        for (int i = 0; i < ids.size() / 2; i++) {
            assertFalse(bitmaps.isSet(getIndex(bitmapIndex).getBitmap(), ids.get(i)));
        }
        for (int i = ids.size() / 2; i < ids.size(); i++) {
            assertTrue(bitmaps.isSet(getIndex(bitmapIndex).getBitmap(), ids.get(i)));
        }
    }

    @Test(dataProvider = "labInvertedIndexDataProviderWithData")
    public void testOr(LABBitmapIndex<RoaringBitmap, RoaringBitmap> bitmapIndex, List<Integer> ids) throws Exception {
        LABBitmaps<RoaringBitmap, RoaringBitmap> bitmaps = new RoaringLABBitmaps();

        int lastId = ids.get(ids.size() - 1);

        bitmapIndex.set(lastId + 2);

        RoaringBitmap bitmap = new RoaringBitmap();
        bitmap.add(lastId + 1);
        bitmap.add(lastId + 3);
        bitmapIndex.or(bitmap);

        for (int id : ids) {
            assertTrue(bitmaps.isSet(getIndex(bitmapIndex).getBitmap(), id));
        }
        assertTrue(bitmaps.isSet(getIndex(bitmapIndex).getBitmap(), lastId + 1));
        assertTrue(bitmaps.isSet(getIndex(bitmapIndex).getBitmap(), lastId + 2));
        assertTrue(bitmaps.isSet(getIndex(bitmapIndex).getBitmap(), lastId + 3));
    }

    @Test(dataProvider = "labInvertedIndexDataProviderWithData")
    public void testOrToSourceSize(LABBitmapIndex<RoaringBitmap, RoaringBitmap> bitmapIndex, List<Integer> ids) throws Exception {
        LABBitmaps<RoaringBitmap, RoaringBitmap> bitmaps = new RoaringLABBitmaps();

        int lastId = ids.get(ids.size() - 1);

        bitmapIndex.set(lastId + 2);

        RoaringBitmap bitmap = new RoaringBitmap();
        bitmap.add(lastId + 1);
        bitmap.add(lastId + 3);
        bitmapIndex.orToSourceSize(bitmap);

        for (int id : ids) {
            assertTrue(bitmaps.isSet(getIndex(bitmapIndex).getBitmap(), id));
        }
        assertTrue(bitmaps.isSet(getIndex(bitmapIndex).getBitmap(), lastId + 1));
        assertTrue(bitmaps.isSet(getIndex(bitmapIndex).getBitmap(), lastId + 2));
        assertTrue(bitmaps.isSet(getIndex(bitmapIndex).getBitmap(), lastId + 3)); // roaring ignores source size requirement
    }

    @Test
    public void testSetIfEmpty() throws Exception {
        LABBitmaps<RoaringBitmap, RoaringBitmap> bitmaps = new RoaringLABBitmaps();

        LABBitmapIndex<RoaringBitmap, RoaringBitmap> index = buildInvertedIndex(bitmaps);

        // setIfEmpty index 1
        index.setIfEmpty(1);
        RoaringBitmap got = getIndex(index).getBitmap();
        assertEquals(got.getCardinality(), 1);
        assertTrue(got.contains(1));

        // setIfEmpty index 2 noops
        index.setIfEmpty(2);
        got = getIndex(index).getBitmap();
        assertEquals(got.getCardinality(), 1);
        assertTrue(got.contains(1));
        assertFalse(got.contains(2));

        // check index 1 is still set after merge
        got = getIndex(index).getBitmap();
        assertEquals(got.getCardinality(), 1);
        assertTrue(got.contains(1));

        // setIfEmpty index 3 noops
        index.setIfEmpty(3);
        got = getIndex(index).getBitmap();
        assertEquals(got.getCardinality(), 1);
        assertTrue(got.contains(1));
        assertFalse(got.contains(3));

        // set index 4
        index.set(4);
        got = getIndex(index).getBitmap();
        assertEquals(got.getCardinality(), 2);
        assertTrue(got.contains(1));
        assertTrue(got.contains(4));

        // remove index 1, 4
        index.remove(1);
        index.remove(4);
        assertFalse(getIndex(index).isSet());

        // setIfEmpty index 5
        index.setIfEmpty(5);
        got = getIndex(index).getBitmap();
        assertEquals(got.getCardinality(), 1);

        // setIfEmpty index 6 noops
        index.setIfEmpty(6);
        got = getIndex(index).getBitmap();
        assertEquals(got.getCardinality(), 1);

    }

    @DataProvider(name = "labInvertedIndexDataProviderWithData")
    public Object[][] labInvertedIndexDataProviderWithData() throws Exception {
        LABBitmaps<RoaringBitmap, RoaringBitmap> bitmaps = new RoaringLABBitmaps();

        LABBitmapIndex<RoaringBitmap, RoaringBitmap> atomizedIndex = buildInvertedIndex(bitmaps);
        atomizedIndex.set(1, 2, 3, 4);

        return new Object[][] {
            { atomizedIndex, Arrays.asList(1, 2, 3, 4) }
        };
    }

    private <BM extends IBM, IBM> LABBitmapIndex<BM, IBM> buildInvertedIndex(LABBitmaps<BM, IBM> bitmaps) throws Exception {
        AtomicLong version = new AtomicLong();
        return new LABBitmapIndex<>(
            (LABBitmapIndexVersionProvider) () -> version.incrementAndGet(),
            bitmaps,
            0,
            new byte[] { 0 },
            buildValueIndex("bitmap"),
            new byte[] { 0 },
            buildValueIndex("term"),
            new Object());
    }

    @Test(groups = "slow", enabled = false, description = "Concurrency test")
    public void testConcurrency() throws Exception {
        LABBitmaps<RoaringBitmap, RoaringBitmap> bitmaps = new RoaringLABBitmaps();
        ExecutorService executorService = Executors.newFixedThreadPool(8);

        final LABBitmapIndex<RoaringBitmap, RoaringBitmap> invertedIndex = buildInvertedIndex(bitmaps);
        final AtomicInteger idProvider = new AtomicInteger();
        final AtomicInteger done = new AtomicInteger();
        final int runs = 10_000;
        final Random random = new Random();

        List<Future<?>> futures = Lists.newArrayListWithCapacity(8);

        futures.add(executorService.submit(() -> {
            while (done.get() < 7) {
                int id = idProvider.incrementAndGet();
                if (id == Integer.MAX_VALUE) {
                    System.out.println("appender hit max value");
                    break;
                }
                if (random.nextBoolean()) {
                    try {
                        invertedIndex.set(id);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            try {
                RoaringBitmap index = getIndex(invertedIndex).getBitmap();
                System.out.println("appender is done, final cardinality=" + index.getCardinality() + " bytes=" + index.getSizeInBytes());
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }));
        System.out.println("started appender");

        for (int i = 0; i < 7; i++) {
            futures.add(executorService.submit(() -> {
                RoaringBitmap other = new RoaringBitmap();
                int r = 0;
                for (int i1 = 0; i1 < runs; i1++) {
                    int size = idProvider.get();
                    while (r < size) {
                        if (random.nextBoolean()) {
                            other.add(r);
                        }
                        r++;
                    }

                    try {
                        RoaringBitmap index = getIndex(invertedIndex).getBitmap();
                        RoaringBitmap container = FastAggregation.and(index, other);
                    } catch (Exception e) {
                        done.incrementAndGet();
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }

                System.out.println("aggregator is done, final cardinality=" + other.getCardinality() + " bytes=" + other.getSizeInBytes());
                done.incrementAndGet();
            }));
            System.out.println("started aggregators");
        }

        for (Future<?> future : futures) {
            future.get();
            System.out.println("got a future");
        }

        System.out.println("all done");
    }

    @Test(groups = "slow", enabled = false, description = "Performance test")
    public void testInMemoryAppenderSpeed() throws Exception {
        LABBitmapIndex<RoaringBitmap, RoaringBitmap> invertedIndex = buildInvertedIndex(new RoaringLABBitmaps());

        Random r = new Random();
        long t = System.currentTimeMillis();
        for (int i = 0; i < 1_000_000; i++) {
            if (r.nextBoolean()) {
                invertedIndex.set(i);
            }
        }

        long elapsed = System.currentTimeMillis() - t;
        RoaringBitmap index = getIndex(invertedIndex).getBitmap();
        System.out.println("cardinality=" + index.getCardinality() + " bytes=" + index.getSizeInBytes() + " elapsed=" + elapsed);
    }


    public static <BM extends IBM, IBM> LABBitmapAndLastId<BM> getIndex(LABBitmapIndex<BM, IBM> invertedIndex) throws Exception {
        LABBitmapAndLastId<BM> container = new LABBitmapAndLastId<>();
        invertedIndex.getIndex(container);
        return container;
    }


    public static ValueIndex<byte[]> buildValueIndex(String name) throws Exception {
        File root = Files.createTempDir();
        ListeningExecutorService executorService = MoreExecutors.newDirectExecutorService();
        AtomicLong globalHeapCostInBytes = new AtomicLong();
        LABStats stats = new LABStats(globalHeapCostInBytes);
        LABFiles labFiles = new LABFiles();
        LABEnvironment environment = new LABEnvironment(stats,
            labFiles,
            LABEnvironment.buildLABSchedulerThreadPool(1),
            LABEnvironment.buildLABCompactorThreadPool(1),
            LABEnvironment.buildLABDestroyThreadPool(1),
            null,
            root,
            new LABHeapPressure(stats,
                executorService,
                name,
                1024 * 1024,
                2 * 1024 * 1024,
                globalHeapCostInBytes,
                LABHeapPressure.FreeHeapStrategy.mostBytesFirst
            ),
            4,
            16,
            LABEnvironment.buildLeapsCache(1_000, 4),
            new StripingBolBufferLocks(2048),
            false,
            false);

        return environment.open(new ValueIndexConfig(name,
            64,
            1024 * 1024,
            -1,
            -1,
            10 * 1024 * 1024,
            "deprecated",
            LABRawhide.NAME,
            MemoryRawEntryFormat.NAME,
            20,
            LABHashIndexType.cuckoo,
            2d,
            true,
            Long.MAX_VALUE));
    }

}