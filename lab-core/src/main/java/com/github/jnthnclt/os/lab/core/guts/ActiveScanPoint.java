package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.collections.bah.LRUConcurrentBAHLinkedHash;
import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.guts.api.Next;
import com.github.jnthnclt.os.lab.core.guts.api.RawEntryStream;
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
    long activeFp = Long.MAX_VALUE;
    long activeOffset = -1;
    boolean activeResult;


    private long fp;
    private BolBuffer entryBuffer;


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

    public void setupPointScan(long fp, BolBuffer entryBuffer) {
        this.entryBuffer = entryBuffer;
        this.fp = fp;
    }

    @Override
    public Next next(RawEntryStream stream, BolBuffer nextHint) throws Exception {
        if (activeOffset != -1) {
            return Next.stopped;
        }
        this.next(fp, entryBuffer, stream);
        return Next.more;
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
                activeResult = stream.stream(entryBuffer);
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
