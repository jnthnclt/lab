package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.collections.bah.LRUConcurrentBAHLinkedHash;
import com.github.jnthnclt.os.lab.core.api.FormatTransformer;
import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import com.github.jnthnclt.os.lab.core.io.PointerReadableByteBufferFile;
import com.github.jnthnclt.os.lab.core.io.api.UIO;
import com.github.jnthnclt.os.lab.core.util.LABLogger;
import com.github.jnthnclt.os.lab.core.util.LABLoggerFactory;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Comparator;

public class ActiveScan {

    private static final LABLogger LOG = LABLoggerFactory.getLogger();

    public static long getInclusiveStartOfRow(PointerReadableByteBufferFile readable,
        Leaps l,
        long cacheKey,
        LRUConcurrentBAHLinkedHash<Leaps>leapsCache,
        byte[] cacheKeyBuffer,
        Rawhide rawhide,
        boolean hashIndexEnabled,
        LABHashIndexType hashIndexType,
        byte hashIndexHashFunctionCount,
        long hashIndexHeadOffset,
        long hashIndexMaxCapacity,
        byte hashIndexLongPrecision,
        FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        BolBuffer bbKey,
        BolBuffer entryBuffer,
        BolBuffer entryKeyBuffer,
        boolean exact) throws Exception {

        long rowIndex = -1;

        if (rawhide.compare(l.lastKey, bbKey) < 0) {
            return rowIndex;
        }

        if (exact && hashIndexEnabled && hashIndexMaxCapacity > 0) {
            if (hashIndexType == LABHashIndexType.linearProbe) {
                long exactRowIndex = getLinearProbe(readable,
                    hashIndexHashFunctionCount,
                    hashIndexHeadOffset,
                    hashIndexMaxCapacity,
                    hashIndexLongPrecision,
                    bbKey,
                    entryBuffer,
                    entryKeyBuffer,
                    readKeyFormatTransformer,
                    readValueFormatTransformer,
                    rawhide);
                if (exactRowIndex >= -1) {
                    return exactRowIndex > -1 ? exactRowIndex - 1 : -1;
                }
            } else if (hashIndexType == LABHashIndexType.cuckoo) {
                long exactRowIndex = getCuckoo(readable,
                    hashIndexHashFunctionCount,
                    hashIndexHeadOffset,
                    hashIndexMaxCapacity,
                    hashIndexLongPrecision,
                    bbKey,
                    entryBuffer,
                    entryKeyBuffer,
                    readKeyFormatTransformer,
                    readValueFormatTransformer,
                    rawhide);
                if (exactRowIndex >= -1) {
                    return exactRowIndex > -1 ? exactRowIndex - 1 : -1;
                }
            }
        }

        Comparator<BolBuffer> byteBufferKeyComparator = rawhide.getBolBufferKeyComparator();
        int cacheMisses = 0;
        int cacheHits = 0;
        while (l != null) {
            Leaps next;
            int index = Arrays.binarySearch(l.keys, bbKey, byteBufferKeyComparator);
            if (index == -(l.fps.length + 1)) {
                rowIndex = binarySearchClosestFP(rawhide,
                    readKeyFormatTransformer,
                    readValueFormatTransformer,
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
                    next = Leaps.read(readKeyFormatTransformer, readable, l.fps[index]);
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


        return rowIndex;
    }

    private static long binarySearchClosestFP(Rawhide rawhide,
        FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
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
            int cmp = rawhide.compareKey(readKeyFormatTransformer, readValueFormatTransformer, entryBuffer, entryKeyBuffer, key);
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

    static private long getLinearProbe(PointerReadableByteBufferFile readable,
        byte hashIndexHashFunctionCount,
        long hashIndexHeadOffset,
        long hashIndexMaxCapacity,
        byte hashIndexLongPrecision,
        BolBuffer compareKey,
        BolBuffer entryBuffer,
        BolBuffer keyBuffer,
        FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        Rawhide rawhide) throws Exception {


        long hashIndex = Math.abs(compareKey.longHashCode() % hashIndexMaxCapacity);

        int i = 0;
        while (i < hashIndexMaxCapacity) {
            long readPointer = hashIndexHeadOffset + (hashIndex * hashIndexLongPrecision);
            long offset = readable.readVPLong(readPointer, hashIndexLongPrecision);
            if (offset == 0L) {
                return -1L;
            } else {
                // since we add one at creation time so zero can be null
                rawhide.rawEntryToBuffer(readable, Math.abs(offset) - 1, entryBuffer);
                int c = rawhide.compareKey(readKeyFormatTransformer, readValueFormatTransformer, entryBuffer, keyBuffer, compareKey);
                if (c > 0) {
                    return -1;
                }
                if (c == 0) {
                    return Math.abs(offset) - 1;
                }
                if (offset > 0) {
                    return -1L;
                }
            }
            i++;
            hashIndex = (++hashIndex) % hashIndexMaxCapacity;
        }
        throw new IllegalStateException("ActiveScan failed to get entry because programming is hard.");

    }

    static private long getCuckoo(PointerReadableByteBufferFile readable,
        byte hashIndexHashFunctionCount,
        long hashIndexHeadOffset,
        long hashIndexMaxCapacity,
        byte hashIndexLongPrecision,
        BolBuffer compareKey,
        BolBuffer entryBuffer,
        BolBuffer keyBuffer,
        FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        Rawhide rawhide) throws Exception {


        long hashCode = compareKey.longMurmurHashCode();
        for (int i = 0; i < hashIndexHashFunctionCount; i++) {
            long hashIndex = Math.abs(hashCode % hashIndexMaxCapacity);
            long readPointer = hashIndexHeadOffset + (hashIndex * hashIndexLongPrecision);
            long offset = readable.readVPLong(readPointer, hashIndexLongPrecision);
            if (offset == 0L) {
                return -1L;
            }
            rawhide.rawEntryToBuffer(readable, Math.abs(offset) - 1, entryBuffer);
            int c = rawhide.compareKey(readKeyFormatTransformer, readValueFormatTransformer, entryBuffer, keyBuffer, compareKey);
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
