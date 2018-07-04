package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.base.BolBuffer;
import com.github.jnthnclt.os.lab.base.UIO;
import com.github.jnthnclt.os.lab.collections.bah.LRUConcurrentBAHLinkedHash;
import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.guts.api.ReadIndex;
import com.github.jnthnclt.os.lab.core.guts.api.Scanner;
import com.github.jnthnclt.os.lab.core.io.PointerReadableByteBufferFile;
import com.github.jnthnclt.os.lab.log.LABLogger;
import com.github.jnthnclt.os.lab.log.LABLoggerFactory;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;

public class ActiveScan {

    private static final LABLogger LOG = LABLoggerFactory.getLogger();

    public static PriorityQueue<InterleavingStreamFeed> indexToFeeds(ReadIndex[] indexs,
        boolean hashIndexEnabled,
        boolean pointFrom,
        byte[] from,
        byte[] to,
        Rawhide rawhide,
        BolBuffer firstKeyHint) throws Exception {

        PriorityQueue<InterleavingStreamFeed> interleavingStreamFeeds = new PriorityQueue<>();

        boolean rowScan = (from == null || from.length == 0) && (to == null || to.length == 0);
        for (int i = 0; i < indexs.length; i++) {
            Scanner scanner = null;
            try {
                if (rowScan) {
                    scanner = indexs[i].rowScan(new BolBuffer(), new BolBuffer());
                } else {
                    scanner = indexs[i].rangeScan(hashIndexEnabled, pointFrom, from, to, new BolBuffer(), new BolBuffer());
                }
                if (scanner != null) {
                    InterleavingStreamFeed e = new InterleavingStreamFeed(i, scanner, rawhide);
                    e.feedNext(firstKeyHint);
                    if (e.nextRawEntry != null) {
                        interleavingStreamFeeds.add(e);
                    } else {
                        e.close();
                    }
                }
            } catch (Throwable t) {
                if (scanner != null) {
                    scanner.close();
                }
                throw t;
            }
        }
        return interleavingStreamFeeds;
    }

    public static long getInclusiveStartOfRow(PointerReadableByteBufferFile readable,
        Leaps l,
        long cacheKey,
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache,
        byte[] cacheKeyBuffer,
        Rawhide rawhide,
        boolean hashIndexEnabled,
        LABHashIndexType hashIndexType,
        byte hashIndexHashFunctionCount,
        long hashIndexHeadOffset,
        long hashIndexMaxCapacity,
        byte hashIndexLongPrecision,
        BolBuffer bbKey,
        BolBuffer entryBuffer,
        BolBuffer entryKeyBuffer,
        boolean exact) throws Exception {

        if (rawhide.compare(l.lastKey, bbKey) < 0) {
            return -1;
        }

        if (exact && hashIndexEnabled && hashIndexMaxCapacity > 0) {
            if (hashIndexType == LABHashIndexType.cuckoo) {

                long exactRowIndex = getCuckoo(readable,
                    hashIndexHashFunctionCount,
                    hashIndexHeadOffset,
                    hashIndexMaxCapacity,
                    hashIndexLongPrecision,
                    bbKey,
                    entryBuffer,
                    entryKeyBuffer,
                    rawhide);
                if (exactRowIndex >= -1) {
                    return exactRowIndex > -1 ? exactRowIndex - 1 : -1;
                }
            }
            if (hashIndexType == LABHashIndexType.fibCuckoo) {

                int twoPower = 63- UIO.chunkPower(hashIndexMaxCapacity,1);

                long exactRowIndex = getFibCuckoo(readable,
                    hashIndexHashFunctionCount,
                    hashIndexHeadOffset,
                    twoPower,
                    hashIndexLongPrecision,
                    bbKey,
                    entryBuffer,
                    entryKeyBuffer,
                    rawhide);
                if (exactRowIndex >= -1) {
                    return exactRowIndex > -1 ? exactRowIndex - 1 : -1;
                }
            }
        }

        return findInclusiveStartOfRow(readable,
            l,
            cacheKey,
            leapsCache,
            cacheKeyBuffer,
            rawhide,
            bbKey,
            entryBuffer,
            entryKeyBuffer,
            exact,
            null);
    }

    public static long findInclusiveStartOfRow(PointerReadableByteBufferFile readable,
        Leaps l,
        long cacheKey,
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache,
        byte[] cacheKeyBuffer,
        Rawhide rawhide,
        BolBuffer bbKey,
        BolBuffer entryBuffer,
        BolBuffer entryKeyBuffer,
        boolean exact,
        Leaps[] lastLeap) throws Exception {

        long rowIndex = -1;
        Comparator<BolBuffer> byteBufferKeyComparator = rawhide.getBolBufferKeyComparator();
        int cacheMisses = 0;
        int cacheHits = 0;
        while (l != null) {
            Leaps next;
            int index = Arrays.binarySearch(l.keys, bbKey, byteBufferKeyComparator);
            if (index == -(l.fps.length + 1)) {
                rowIndex = binarySearchClosestFP(rawhide,
                    readable,
                    l,
                    bbKey,
                    entryBuffer,
                    entryKeyBuffer,
                    exact);
                break;
            } else {
                if (index < 0) {
                    index = -(index + 1);
                }

                UIO.longBytes(cacheKey, cacheKeyBuffer, 0);
                UIO.longBytes(l.fps[index], cacheKeyBuffer, 8);

                next = leapsCache.get(cacheKeyBuffer);
                if (next == null) {
                    next = Leaps.read(readable, l.fps[index]);
                    leapsCache.put(Arrays.copyOf(cacheKeyBuffer, 16), next);
                    cacheMisses++;
                } else {
                    cacheHits++;
                }
            }
            l = next;
        }

        LOG.inc("LAB>leapCache>calls");
        if (cacheHits > 0) {
            LOG.inc("LAB>leapCache>hits", cacheHits);
        }
        if (cacheMisses > 0) {
            LOG.inc("LAB>leapCache>misses", cacheMisses);
        }
        if (lastLeap != null) {
            lastLeap[0] = l;
        }
        return rowIndex;
    }

    private static long binarySearchClosestFP(Rawhide rawhide,
        PointerReadableByteBufferFile readable,
        Leaps leaps,
        BolBuffer key,
        BolBuffer entryBuffer,
        BolBuffer entryKeyBuffer,
        boolean exact) throws Exception {

        LongBuffer startOfEntryBuffer = leaps.startOfEntry.get(readable);

        int low = 0;
        int high = startOfEntryBuffer.limit() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long fp = startOfEntryBuffer.get(mid);

            rawhide.rawEntryToBuffer(readable, fp + 1, entryBuffer);
            int cmp = rawhide.compareKey(entryBuffer, entryKeyBuffer, key);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return fp;
            }
        }
        if (exact) {
            return -1;
        } else {
            return startOfEntryBuffer.get(low);
        }
    }

    static private long getCuckoo(PointerReadableByteBufferFile readable,
        byte hashIndexHashFunctionCount,
        long hashIndexHeadOffset,
        long hashIndexMaxCapacity,
        byte hashIndexLongPrecision,
        BolBuffer compareKey,
        BolBuffer entryBuffer,
        BolBuffer keyBuffer,
        Rawhide rawhide) throws Exception {


        long hashCode = compareKey.longMurmurHashCode();
        for (int i = 0; i < hashIndexHashFunctionCount; i++) {
            long hashIndex = Math.abs(LABAppendableIndex.moduloIndexForHash(hashCode, hashIndexMaxCapacity));
            long readPointer = hashIndexHeadOffset + (hashIndex * hashIndexLongPrecision);
            long offset = readable.readVPLong(readPointer, hashIndexLongPrecision);
            if (offset == 0L) {
                return -1L;
            }
            rawhide.rawEntryToBuffer(readable, Math.abs(offset) - 1, entryBuffer);
            int c = rawhide.compareKey(entryBuffer, keyBuffer, compareKey);
            if (c == 0) {
                return Math.abs(offset) - 1;
            }
            if (offset > 0) {
                return -1;
            }
            hashCode = compareKey.longMurmurHashCode(hashCode);
        }
        return -1;
    }

    static private long getFibCuckoo(PointerReadableByteBufferFile readable,
        byte hashIndexHashFunctionCount,
        long hashIndexHeadOffset,
        int twoPower,
        byte hashIndexLongPrecision,
        BolBuffer compareKey,
        BolBuffer entryBuffer,
        BolBuffer keyBuffer,
        Rawhide rawhide) throws Exception {


        long hashCode = compareKey.longMurmurHashCode();
        for (int i = 0; i < hashIndexHashFunctionCount; i++) {
            long hashIndex = Math.abs(LABAppendableIndex.fibonacciIndexForHash(hashCode, twoPower));
            long readPointer = hashIndexHeadOffset + (hashIndex * hashIndexLongPrecision);
            long offset = readable.readVPLong(readPointer, hashIndexLongPrecision);
            if (offset == 0L) {
                return -1L;
            }
            rawhide.rawEntryToBuffer(readable, Math.abs(offset) - 1, entryBuffer);
            int c = rawhide.compareKey(entryBuffer, keyBuffer, compareKey);
            if (c == 0) {
                return Math.abs(offset) - 1;
            }
            if (offset > 0) {
                return -1;
            }
            hashCode = compareKey.longMurmurHashCode(hashCode);
        }
        return -1;
    }

}
