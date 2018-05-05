package com.github.jnthnclt.os.lab.core;

import com.github.jnthnclt.os.lab.core.api.FormatTransformer;
import com.github.jnthnclt.os.lab.core.api.rawhide.LABRawhide;
import com.github.jnthnclt.os.lab.core.guts.ActiveScan;
import com.github.jnthnclt.os.lab.core.guts.CompactableIndexes;
import com.github.jnthnclt.os.lab.core.guts.InterleaveStream;
import com.github.jnthnclt.os.lab.core.guts.LABHashIndexType;
import com.github.jnthnclt.os.lab.core.guts.PointInterleave;
import com.github.jnthnclt.os.lab.core.guts.api.Next;
import com.github.jnthnclt.os.lab.core.guts.api.RawAppendableIndex;
import com.github.jnthnclt.os.lab.core.guts.api.RawEntryStream;
import com.github.jnthnclt.os.lab.core.guts.api.Scanner;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import com.github.jnthnclt.os.lab.core.io.api.UIO;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.Assert;

/**
 * @author jonathan.colt
 */
public class TestUtils {

    public static LABHashIndexType indexType = LABHashIndexType.cuckoo;

    private static final AtomicLong timeProvider = new AtomicLong();

    public static long append(Random rand,
        RawAppendableIndex index,
        long start,
        int step,
        int count,
        ConcurrentSkipListMap<byte[], byte[]> desired,
        BolBuffer keyBuffer) throws Exception {

        long[] lastKey = new long[1];
        index.append((stream) -> {
            long k = start;
            for (int i = 0; i < count; i++) {
                k += 1 + rand.nextInt(step);

                byte[] key = UIO.longBytes(k, new byte[8], 0);
                long time = timeProvider.incrementAndGet();

                long specialK = k;
                if (desired != null) {
                    desired.compute(key, (t, u) -> {
                        if (u == null) {
                            return rawEntry(specialK, time);
                        } else {
                            return value(u) > time ? u : rawEntry(specialK, time);
                        }
                    });
                }
                byte[] rawEntry = rawEntry(k, time);
                if (!stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, new BolBuffer(rawEntry))) {
                    break;
                }
            }
            lastKey[0] = k;
            return true;
        }, keyBuffer);
        return lastKey[0];
    }

    public static byte[] rawEntry(long k, long time) {
        BolBuffer bolBuffer = new BolBuffer();
        try {
            LABRawhide.SINGLETON.toRawEntry(UIO.longBytes(k), time, false, 0, UIO.longBytes(time), bolBuffer);
        } catch (IOException ex) {
            Assert.fail("barf", ex);
        }
        return bolBuffer.copy();
    }

    public static long key(byte[] entry) {
        return UIO.bytesLong(entry, 4);
    }

    public static long key(BolBuffer entry) {
        if (entry == null) {
            return -1;
        }
        return entry.getLong(4);
    }

    public static long value(byte[] entry) {
        return UIO.bytesLong(entry, 4 + 8 + 8 + 1 + 8 + 4);
    }

    public static long value(BolBuffer entry) {
        if (entry == null) {
            return -1;
        }
        return entry.getLong(4 + 8 + 8 + 1 + 8 + 4);
    }

    public static String toString(byte[] entry) {
        return "key:" + key(entry) + " value:" + value(entry) + " timestamp:" + LABRawhide.SINGLETON.timestamp(FormatTransformer.NO_OP, FormatTransformer.NO_OP,
            new BolBuffer(entry));
    }

    public static String toString(BolBuffer entry) {
        return "key:" + key(entry) + " value:" + value(entry) + " timestamp:" + LABRawhide.SINGLETON.timestamp(FormatTransformer.NO_OP, FormatTransformer.NO_OP,
            entry);
    }

    public static void assertions(CompactableIndexes indexes,
        int count, int step,
        ConcurrentSkipListMap<byte[], byte[]> desired) throws
        Exception {

        ArrayList<byte[]> keys = new ArrayList<>(desired.navigableKeySet());

        int[] index = new int[1];

        indexes.tx(-1, null, null, (index1, fromKey, toKey, acquired, hydrateValues) -> {
            Scanner rowScan = new InterleaveStream(LABRawhide.SINGLETON,
                ActiveScan.indexToFeeds(acquired, null, null, LABRawhide.SINGLETON));
            try {
                RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                    //System.out.println("scanned:" + UIO.bytesLong(keys.get(index[0])) + " " + key(rawEntry));
                    Assert.assertEquals(UIO.bytesLong(keys.get(index[0])), key(rawEntry));
                    index[0]++;
                    return true;
                };
                while (rowScan.next(stream,null) == Next.more) {
                }
            } finally {
                rowScan.close();
            }
            //System.out.println("rowScan PASSED");
            return true;
        }, true);

        indexes.tx(-1, null, null, (index1, fromKey, toKey, acquired, hydrateValues) -> {
            for (int i = 0; i < count * step; i++) {
                long k = i;
                PointInterleave pointInterleave = new PointInterleave(acquired, UIO.longBytes(k, new byte[8], 0), LABRawhide.SINGLETON, true);
                RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                    byte[] expectedFP = desired.get(UIO.longBytes(key(rawEntry), new byte[8], 0));
                    if (expectedFP == null) {
                        Assert.assertTrue(expectedFP == null && value(rawEntry) == -1);
                    } else {
                        Assert.assertEquals(value(expectedFP), value(rawEntry));
                    }
                    return true;
                };

                while (pointInterleave.next(stream,null) == Next.more) {
                }
            }
            //System.out.println("gets PASSED");
            return true;
        }, true);

        indexes.tx(-1, null, null, (index1, fromKey, toKey, acquired, hydrateValues) -> {
            for (int i = 0; i < keys.size() - 3; i++) {
                int _i = i;

                int[] streamed = new int[1];
                RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                    if (value(rawEntry) > -1) {
                        //System.out.println("Streamed:" + key(rawEntry));
                        streamed[0]++;
                    }
                    return true;
                };

                //System.out.println("Asked:" + UIO.bytesLong(keys.get(_i)) + " to " + UIO.bytesLong(keys.get(_i + 3)));
                Scanner rangeScan = new InterleaveStream(LABRawhide.SINGLETON,
                    ActiveScan.indexToFeeds(acquired, keys.get(_i), keys.get(_i + 3), LABRawhide.SINGLETON));
                try {
                    while (rangeScan.next(stream,null) == Next.more) {
                    }
                } finally {
                    rangeScan.close();
                }
                Assert.assertEquals(3, streamed[0]);
            }

            //System.out.println("rangeScan PASSED");
            return true;
        }, true);

        indexes.tx(-1, null, null, (index1, fromKey, toKey, acquired, hydrateValues) -> {
            for (int i = 0; i < keys.size() - 3; i++) {
                int _i = i;
                int[] streamed = new int[1];
                RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                    if (value(rawEntry) > -1) {
                        streamed[0]++;
                    }
                    return true;
                };
                Scanner rangeScan = new InterleaveStream(LABRawhide.SINGLETON,
                    ActiveScan.indexToFeeds(acquired, UIO.longBytes(UIO.bytesLong(keys.get(_i)) + 1, new byte[8], 0), keys.get(_i + 3),
                    LABRawhide.SINGLETON));
                try {
                    while (rangeScan.next(stream,null) == Next.more) {
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
