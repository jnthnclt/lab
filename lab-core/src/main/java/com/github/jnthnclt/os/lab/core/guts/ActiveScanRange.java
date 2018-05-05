package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.collections.bah.LRUConcurrentBAHLinkedHash;
import com.github.jnthnclt.os.lab.core.api.FormatTransformer;
import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.guts.api.Next;
import com.github.jnthnclt.os.lab.core.guts.api.RawEntryStream;
import com.github.jnthnclt.os.lab.core.guts.api.Scanner;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import com.github.jnthnclt.os.lab.core.io.PointerReadableByteBufferFile;

/**
 * @author jonathan.colt
 */
public class ActiveScanRange implements Scanner {

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


    private long fp;
    private byte[] to;
    private BolBuffer entryKeyBuffer;
    private BolBuffer bbFrom;
    private BolBuffer bbTo;


    public ActiveScanRange() {
    }

    public long getInclusiveStartOfRow(BolBuffer bbKey, BolBuffer entryBuffer, BolBuffer entryKeyBuffer, boolean exact) throws Exception {

        return ActiveScan.getInclusiveStartOfRow(readable,
            leaps,
            cacheKey,
            leapsCache,
            cacheKeyBuffer,
            rawhide,
            false,
            hashIndexType,
            hashIndexHashFunctionCount,
            hashIndexHeadOffset,
            hashIndexMaxCapacity,
            hashIndexLongPrecision,
            readKeyFormatTransformer,
            readValueFormatTransformer,
            bbKey,
            entryBuffer,
            entryKeyBuffer,
            exact);
    }



    public void setupAsRangeScanner(long fp,
        byte[] to,
        BolBuffer entryBuffer,
        BolBuffer entryKeyBuffer,
        BolBuffer bbFrom,
        BolBuffer bbTo) {

        this.fp = fp;
        this.to = to;
        this.entryKeyBuffer = entryKeyBuffer;
        this.bbFrom = bbFrom;
        this.bbTo = bbTo;
    }

    private boolean result() {
        return activeResult;
    }


    @Override
    public Next next(RawEntryStream stream, BolBuffer nextHint) throws Exception {
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
