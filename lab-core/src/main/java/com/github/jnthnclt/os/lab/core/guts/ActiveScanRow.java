package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.base.BolBuffer;
import com.github.jnthnclt.os.lab.collections.bah.LRUConcurrentBAHLinkedHash;
import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.guts.api.Scanner;
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
    final Leaps[] leaps = new Leaps[1];
    final BolBuffer entryKeyBuffer = new BolBuffer();

    public ActiveScanRow() {
    }

    @Override
    public BolBuffer next(BolBuffer rawEntry, BolBuffer nextKeyHint) throws Exception {

        if (nextKeyHint != null) {

//            if (leaps[0] == null || rawhide.compare(leaps[0].lastKey, nextKeyHint) < 0) {
//                return null;
//            }
//
//            long fp = ActiveScan.findInclusiveStartOfRow(readable,
//                leaps[0],
//                cacheKey,
//                leapsCache,
//                cacheKeyBuffer,
//                rawhide,
//                nextKeyHint,
//                rawEntry,
//                entryKeyBuffer,
//                false,
//                leaps);
//            if (fp == -1) {
//                leaps[0] = null;
//                activeOffset = -1;
//                return null;
//            }
//
//            int type = readable.read(fp);
//            if (type == LABAppendableIndex.ENTRY) {
//                rawhide.rawEntryToBuffer(readable, fp + 1, rawEntry);
//                return rawEntry;
//            }
//            return null;



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

                    if (rawhide.compare(leaps.lastKey, nextKeyHint) >= 0) {

                        long fp = ActiveScan.findInclusiveStartOfRow(readable,
                            leaps,
                            cacheKey,
                            leapsCache,
                            cacheKeyBuffer,
                            rawhide,
                            nextKeyHint,
                            rawEntry,
                            entryKeyBuffer,
                            false,
                            null);
                        if (fp == -1) {
                            activeOffset = -1;
                            return null;
                        }
                        activeOffset = fp;
                    } else {
                        int length = readable.readInt(activeOffset); // entryLength
                        activeOffset += (length);
                    }

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
