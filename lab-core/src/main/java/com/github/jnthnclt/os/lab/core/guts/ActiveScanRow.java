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
public class ActiveScanRow implements Scanner {

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

    private BolBuffer entryBuffer;

    public ActiveScanRow() {
    }

    public void setupRowScan(BolBuffer entryBuffer) {
        this.entryBuffer = entryBuffer;
    }

    private boolean result() {
        return activeResult;
    }


    @Override
    public Next next(RawEntryStream stream, BolBuffer nextHint) throws Exception {
        this.next(0, entryBuffer, stream);
        boolean more = this.result();
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
