package com.github.jnthnclt.os.lab.core.example;

import com.github.jnthnclt.os.lab.collections.bah.LRUConcurrentBAHLinkedHash;
import com.github.jnthnclt.os.lab.core.LABEnvironment;
import com.github.jnthnclt.os.lab.core.LABHeapPressure;
import com.github.jnthnclt.os.lab.core.LABHeapPressure.FreeHeapStrategy;
import com.github.jnthnclt.os.lab.core.LABStats;
import com.github.jnthnclt.os.lab.core.api.MemoryRawEntryFormat;
import com.github.jnthnclt.os.lab.core.api.ValueIndex;
import com.github.jnthnclt.os.lab.core.api.ValueIndexConfig;
import com.github.jnthnclt.os.lab.core.api.ValueStream;
import com.github.jnthnclt.os.lab.core.api.rawhide.LABRawhide;
import com.github.jnthnclt.os.lab.core.guts.LABHashIndexType;
import com.github.jnthnclt.os.lab.core.guts.Leaps;
import com.github.jnthnclt.os.lab.core.guts.StripingBolBufferLocks;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import com.github.jnthnclt.os.lab.core.io.api.UIO;
import com.google.common.io.Files;
import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class HelloLAB {

    public static AtomicLong version = new AtomicLong();

    public static void main(String[] args) throws Exception {

        File root = Files.createTempDir();
        AtomicLong globalHeapCostInBytes = new AtomicLong();
        LABStats stats = new LABStats(globalHeapCostInBytes);

        LABEnvironment labEnvironment = buildEnv(globalHeapCostInBytes, stats, root);
        ValueIndex<byte[]> index = buildValueIndex(labEnvironment, "foo");
        update(index, false, 100, 2, true);
        index.commitAndWait(0, true);

        ValueStream valueStream = (index1, key, timestamp, tombstoned, version, payload) -> {
            String keyString = key == null ? null : String.valueOf(key.getInt(0));
            String valueString = payload == null ? null : String.valueOf(payload.getInt(0));
            System.out.println(
                "\tindex:" + index1 + " key:" + keyString + " value:" + valueString + " timestamp:" + timestamp + " version:" + version + " tombstoned:" +
                    tombstoned);
            return true;
        };

        pointGet(index, 16, valueStream);
        pointRangeScan(index, 16, 32, valueStream);
        pointRangeScan(index, 15, 32, valueStream); // no results because 15 isn't a valid point start of scan
        rangeScan(index, 15, 32, valueStream);

        update(index, true, 100, 16, true);
        index.commitAndWait(0, true);

        pointGet(index, 16, valueStream);
        pointRangeScan(index, 16, 32, valueStream);
        pointRangeScan(index, 15, 32, valueStream); // no results because 15 isn't a valid point start of scan
        rangeScan(index, 15, 32, valueStream);

        index.rowScan(valueStream, true);

        labEnvironment.shutdown();
    }


    private static void update(ValueIndex<byte[]> index, boolean delete, int count, int modulo, boolean fsync) throws Exception {
        BolBuffer rawEntryBuffer = new BolBuffer();
        BolBuffer keyBuffer = new BolBuffer();
        long timestamp = System.currentTimeMillis();
        index.append((stream) -> {

            for (int i = 0; i < count; i++) {
                if (i % modulo == 0) {
                    stream.stream(i, UIO.intBytes(i), timestamp, delete, version.incrementAndGet(), UIO.intBytes(i));
                }
            }
            return true;
        }, fsync, rawEntryBuffer, keyBuffer);
    }

    private static void pointGet(ValueIndex<byte[]> index, int key, ValueStream valueStream) throws Exception {
        System.out.println("pointGet for key:" + key);
        byte[] keyBytes = UIO.intBytes(key);
        index.get(
            (keyStream) -> {
                keyStream.key(0, keyBytes, 0, keyBytes.length);
                return true;
            },
            valueStream,
            true);

    }

    private static void pointRangeScan(ValueIndex<byte[]> index, int from, int to, ValueStream valueStream) throws Exception {
        System.out.println("pointRangeScan for for from:" + from + " to:" + to);

        byte[] fromBytes = from == -1 ? null : UIO.intBytes(from);
        byte[] toBytes = to == -1 ? null : UIO.intBytes(to);

        index.pointRangeScan(fromBytes, toBytes, valueStream, true);
    }

    private static void rangeScan(ValueIndex<byte[]> index, int from, int to, ValueStream valueStream) throws Exception {
        System.out.println("rangeScan for for from:" + from + " to:" + to);

        byte[] fromBytes = from == -1 ? null : UIO.intBytes(from);
        byte[] toBytes = to == -1 ? null : UIO.intBytes(to);

        index.rangeScan(fromBytes, toBytes, valueStream, true);
    }


    private static LABEnvironment buildEnv(AtomicLong globalHeapCostInBytes,
        LABStats stats,
        File root) throws Exception {


        ExecutorService labHeapSchedulerThreadPool = LABEnvironment.buildLABHeapSchedulerThreadPool(1);


        String name = "default";
        int maxHeapPressureInBytes = 1024 * 1024 * 10;
        int blockOnHeapPressureInBytes = 1024 * 1024 * 10;
        FreeHeapStrategy freeHeapStrategy = FreeHeapStrategy.mostBytesFirst;

        LABHeapPressure labHeapPressure = new LABHeapPressure(stats,
            labHeapSchedulerThreadPool,
            name,
            maxHeapPressureInBytes,
            blockOnHeapPressureInBytes,
            globalHeapCostInBytes,
            freeHeapStrategy);

        int maxCapacity = 100;
        int concurrency = 8;
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(maxCapacity, concurrency);


        return new LABEnvironment(stats,
            null,
            LABEnvironment.buildLABSchedulerThreadPool(1),
            LABEnvironment.buildLABCompactorThreadPool(4),
            LABEnvironment.buildLABDestroyThreadPool(1),
            null,
            root,
            labHeapPressure,
            1,
            2,
            leapsCache,
            new StripingBolBufferLocks(1024),
            true,
            false);
    }

    private static ValueIndex<byte[]> buildValueIndex(LABEnvironment env, String name) throws Exception {

        long splitAfterSizeInBytes = 64 * 1024 * 1024;
        int entriesBetweenLeaps = 4096;
        int maxHeapPressureInBytes = 1024 * 1024 * 10;
        LABHashIndexType hashIndexType = LABHashIndexType.cuckoo;
        int entryLengthPower = 27;
        boolean hashIndexEnabled = true;
        double hashIndexLoadFactor = 2d;

        ValueIndexConfig valueIndexConfig = new ValueIndexConfig(name,
            entriesBetweenLeaps,
            maxHeapPressureInBytes,
            splitAfterSizeInBytes,
            -1,
            -1,
            "deprecated",
            LABRawhide.NAME,
            MemoryRawEntryFormat.NAME,
            entryLengthPower,
            hashIndexType,
            hashIndexLoadFactor,
            hashIndexEnabled,
            Long.MAX_VALUE);

        return env.open(valueIndexConfig);
    }


}
