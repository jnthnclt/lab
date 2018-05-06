package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.collections.bah.LRUConcurrentBAHLinkedHash;
import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.guts.api.Scanner;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import com.github.jnthnclt.os.lab.core.io.PointerReadableByteBufferFile;

/**
 * @author jonathan.colt
 */
public class ActiveScanRow implements Scanner {

    Rawhide rawhide;
    PointerReadableByteBufferFile readable;
    long cacheKey;
    LRUConcurrentBAHLinkedHash<Leaps> leapsCache;
    byte[] cacheKeyBuffer;
    long activeOffset = 0;

    public ActiveScanRow() {
    }

    @Override
    public BolBuffer next(BolBuffer rawEntry, BolBuffer nextKeyHint) throws Exception {

        if (nextKeyHint != null) {

            BolBuffer entryKeyBuffer = new BolBuffer();

            int type;
            while ((type = readable.read(activeOffset)) >= 0) {
                activeOffset++;
                if (type == LABAppendableIndex.ENTRY) {
                    activeOffset += rawhide.rawEntryToBuffer(readable, activeOffset, rawEntry);

                    int c = rawhide.compareKey(rawEntry, entryKeyBuffer, nextKeyHint);
                    if (c < 0) {
                        continue;
                    }
                    return rawEntry;
                } else if (type == LABAppendableIndex.LEAP) {
                    Leaps leaps = Leaps.read(readable, activeOffset);
                    long fp = ActiveScan.findInclusiveStartOfRow(readable,
                        leaps,
                        cacheKey,
                        leapsCache,
                        cacheKeyBuffer,
                        rawhide,
                        nextKeyHint,
                        rawEntry,
                        entryKeyBuffer,
                        false);
                    if (fp == -1) {
                        return null;
                    }
                    activeOffset = fp;
                } else if (type == LABAppendableIndex.FOOTER) {
                    return null;
                } else {
                    throw new IllegalStateException("Bad row type:" + type + " at fp:" + (activeOffset - 1));
                }
            }
            throw new IllegalStateException("Missing footer");

        } else {

            int type;
            while ((type = readable.read(activeOffset)) >= 0) {
                activeOffset++;
                if (type == LABAppendableIndex.ENTRY) {
                    activeOffset += rawhide.rawEntryToBuffer(readable, activeOffset, rawEntry);
                    return rawEntry;
                } else if (type == LABAppendableIndex.LEAP) {
                    activeOffset += readable.readInt(activeOffset); // entryLength
                } else if (type == LABAppendableIndex.FOOTER) {
                    return null;
                } else {
                    throw new IllegalStateException("Bad row type:" + type + " at fp:" + (activeOffset - 1));
                }
            }
            throw new IllegalStateException("Missing footer");
        }
    }


    @Override
    public void close() throws Exception {
    }


}
