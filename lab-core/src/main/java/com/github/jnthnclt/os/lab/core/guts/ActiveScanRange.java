package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.collections.bah.LRUConcurrentBAHLinkedHash;
import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.guts.api.Scanner;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import com.github.jnthnclt.os.lab.core.io.PointerReadableByteBufferFile;

/**
 * @author jonathan.colt
 */
public class ActiveScanRange implements Scanner {

    Rawhide rawhide;
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

    long activeOffset = -1;

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

        this.activeOffset = fp;
        this.to = to;
        this.entryKeyBuffer = entryKeyBuffer;
        this.bbFrom = bbFrom;
        this.bbTo = bbTo;
    }

    private boolean bbFromPasses = false;

    @Override
    public BolBuffer next(BolBuffer rawEntry, BolBuffer nextHint) throws Exception {
        while(true) {
            BolBuffer next = next(rawEntry);
            if (next == null) {
                return null;
            }
            if (bbFromPasses) {
                int c = to == null ? -1 : rawhide.compareKey(next, entryKeyBuffer, bbTo);
                if (c < 0) {
                    return next;
                } else {
                    return null;
                }
            } else {
                int c = rawhide.compareKey(next, entryKeyBuffer, bbFrom);
                if (c >= 0) {
                    bbFromPasses = true;
                    c = to == null ? -1 : rawhide.compareKey(next, entryKeyBuffer, bbTo);
                    if (c < 0) {
                        return next;
                    } else {
                        return null;
                    }
                }
            }
        }
    }


    private BolBuffer next(BolBuffer entryBuffer) throws Exception {

        int type;
        while ((type = readable.read(activeOffset)) >= 0) {
            activeOffset++;
            if (type == LABAppendableIndex.ENTRY) {
                activeOffset += rawhide.rawEntryToBuffer(readable, activeOffset, entryBuffer);
                return entryBuffer;
            } else if (type == LABAppendableIndex.LEAP) {
                int length = readable.readInt(activeOffset); // entryLength
                activeOffset += (length);
            } else if (type == LABAppendableIndex.FOOTER) {
                return null;
            } else {
                throw new IllegalStateException("Bad row type:" + type + " at fp:" + (activeOffset - 1));
            }
        }
        throw new IllegalStateException("Missing footer");
    }

    @Override
    public void close() throws Exception {
    }



}
