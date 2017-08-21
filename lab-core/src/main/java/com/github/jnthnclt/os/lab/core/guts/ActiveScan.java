package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.collections.bah.LRUConcurrentBAHLinkedHash;
import com.github.jnthnclt.os.lab.core.api.FormatTransformer;
import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.guts.api.Next;
import com.github.jnthnclt.os.lab.core.guts.api.RawEntryStream;
import com.github.jnthnclt.os.lab.core.guts.api.Scanner;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import com.github.jnthnclt.os.lab.core.io.PointerReadableByteBufferFile;
import com.github.jnthnclt.os.lab.core.io.api.UIO;
import com.github.jnthnclt.os.lab.core.util.LABLogger;
import com.github.jnthnclt.os.lab.core.util.LABLoggerFactory;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Comparator;

/**
 * @author jonathan.colt
 */
public class ActiveScan implements Scanner {

    private static final LABLogger LOG = LABLoggerFactory.getLogger();;

    private final boolean hashIndexEnabled;

    Rawhide rawhide;
    FormatTransformer readKeyFormatTransformer;
    FormatTransformer readValueFormatTransformer;
    Leaps leaps;
    long cacheKey;
    LRUConcurrentBAHLinkedHash<Leaps> leapsCache;
    PointerReadableByteBufferFile readable;
    byte[] cacheKeyBuffer;

    LABHashIndexType hashIndexType;
    byte hashIndexHashFunctionCount;
    long hashIndexHeadOffset;
    long hashIndexMaxCapacity;
    byte hashIndexLongPrecision;
    long activeFp = Long.MAX_VALUE;
    long activeOffset = -1;
    boolean activeResult;


    private enum ScanType {
        point, range, row
    }

    private ScanType scanType;
    private long fp;
    private byte[] to;
    private BolBuffer entryBuffer;
    private BolBuffer entryKeyBuffer;
    private BolBuffer bbFrom;
    private BolBuffer bbTo;


    public ActiveScan(boolean hashIndexEnabled) {
        this.hashIndexEnabled = hashIndexEnabled;
    }

    public long getInclusiveStartOfRow(BolBuffer bbKey, BolBuffer entryBuffer, BolBuffer entryKeyBuffer, boolean exact) throws Exception {
        Leaps l = leaps;
        long rowIndex = -1;

        if (rawhide.compare(l.lastKey, bbKey) < 0) {
            return rowIndex;
        }

        if (exact && hashIndexEnabled && hashIndexMaxCapacity > 0) {
            if (hashIndexType == LABHashIndexType.linearProbe) {
                long exactRowIndex = getLinearProbe(bbKey, entryBuffer, entryKeyBuffer, readKeyFormatTransformer, readValueFormatTransformer, rawhide);
                if (exactRowIndex >= -1) {
                    return exactRowIndex > -1 ? exactRowIndex - 1 : -1;
                }
            } else if (hashIndexType == LABHashIndexType.cuckoo) {
                long exactRowIndex = getCuckoo(bbKey, entryBuffer, entryKeyBuffer, readKeyFormatTransformer, readValueFormatTransformer, rawhide);
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

    private long getLinearProbe(BolBuffer compareKey,
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

    private long getCuckoo(BolBuffer compareKey,
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


    public void setupAsRangeScanner(long fp, byte[] to, BolBuffer entryBuffer, BolBuffer entryKeyBuffer, BolBuffer bbFrom, BolBuffer bbTo) {
        this.scanType = ScanType.range;
        this.fp = fp;
        this.to = to;
        this.entryBuffer = entryBuffer;
        this.entryKeyBuffer = entryKeyBuffer;
        this.bbFrom = bbFrom;
        this.bbTo = bbTo;
    }

    public void setupRowScan(BolBuffer entryBuffer) {
        this.scanType = ScanType.row;
        this.entryBuffer = entryBuffer;
    }

    public void setupPointScan(long fp, BolBuffer entryBuffer) {
        this.scanType = ScanType.point;
        this.entryBuffer = entryBuffer;
        this.fp = fp;
    }

    private boolean result() {
        return activeResult;
    }


    @Override
    public Next next(RawEntryStream stream) throws Exception {
        if (scanType == ScanType.point) {
            if (activeOffset != -1) {
                return Next.stopped;
            }
            this.next(fp, entryBuffer, stream);
            return Next.more;
        } else if (scanType == ScanType.range) {
            BolBuffer entryBuffer = new BolBuffer();
            boolean[] once = new boolean[] { false };
            boolean more = true;
            while (!once[0] && more) {
                more = this.next(fp,
                    entryBuffer,
                    (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                        int c = rawhide.compareKey(readKeyFormatTransformer, readValueFormatTransformer, rawEntry, entryKeyBuffer, bbFrom);
                        if (c >= 0) {
                            c = to == null ? -1 : rawhide.compareKey(readKeyFormatTransformer, readValueFormatTransformer, rawEntry, entryKeyBuffer, bbTo);
                            if (c < 0) {
                                once[0] = true;
                            }
                            return c < 0 && stream.stream(readKeyFormatTransformer, readValueFormatTransformer, rawEntry);
                        } else {
                            return true;
                        }
                    });
            }
            more = this.result();
            return more ? Next.more : Next.stopped;
        } else if (scanType == ScanType.row) {
            this.next(0, entryBuffer, stream);
            boolean more = this.result();
            return more ? Next.more : Next.stopped;
        } else {
            throw new IllegalStateException("This has not been setup as a scanner.");
        }
    }

    @Override
    public void close() throws Exception {
    }


    private boolean next(long fp, BolBuffer entryBuffer, RawEntryStream stream) throws Exception {

        if (activeFp == Long.MAX_VALUE || activeFp != fp) {
            activeFp = fp;
            activeOffset = fp;
        }
        activeResult = false;
        int type;
        while ((type = readable.read(activeOffset)) >= 0) {
            activeOffset++;
            if (type == LABAppendableIndex.ENTRY) {
                activeOffset += rawhide.rawEntryToBuffer(readable, activeOffset, entryBuffer);
                activeResult = stream.stream(readKeyFormatTransformer, readValueFormatTransformer, entryBuffer);
                return false;
            } else if (type == LABAppendableIndex.LEAP) {
                int length = readable.readInt(activeOffset); // entryLength
                activeOffset += (length);
            } else if (type == LABAppendableIndex.FOOTER) {
                activeResult = false;
                return false;
            } else {
                throw new IllegalStateException("Bad row type:" + type + " at fp:" + (activeOffset - 1));
            }
        }
        throw new IllegalStateException("Missing footer");
    }

}
