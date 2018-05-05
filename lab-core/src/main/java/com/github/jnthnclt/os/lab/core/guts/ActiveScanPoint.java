package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.collections.bah.LRUConcurrentBAHLinkedHash;
import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.guts.api.Scanner;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import com.github.jnthnclt.os.lab.core.io.PointerReadableByteBufferFile;

/**
 * @author jonathan.colt
 */
public class ActiveScanPoint implements Scanner {

    private final boolean hashIndexEnabled;

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


    public ActiveScanPoint(boolean hashIndexEnabled) {
        this.hashIndexEnabled = hashIndexEnabled;
    }

    public long getInclusiveStartOfRow(BolBuffer bbKey, BolBuffer entryBuffer, BolBuffer entryKeyBuffer, boolean exact) throws Exception {
        return ActiveScan.getInclusiveStartOfRow(readable,
            leaps,
            cacheKey,
            leapsCache,
            cacheKeyBuffer,
            rawhide,
            hashIndexEnabled,
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

    public void setupPointScan(long fp) {
        this.activeOffset = fp;
    }

    private boolean once = false;

    @Override
    public BolBuffer next(BolBuffer rawEntry, BolBuffer nextHint) throws Exception {
        if (once) {
            return null;
        }
        BolBuffer next = next(rawEntry);
        once = true;
        return next;
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
