package com.github.jnthnclt.os.lab.core.guts;

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
    long activeOffset = 0;

    public ActiveScanRow() {
    }

    @Override
    public BolBuffer next(BolBuffer rawEntry, BolBuffer nextHint) throws Exception {
        return this.next(rawEntry);
    }

    private BolBuffer next(BolBuffer entryBuffer) throws Exception {

        int type;
        while ((type = readable.read(activeOffset)) >= 0) {
            activeOffset++;
            if (type == LABAppendableIndex.ENTRY) {
                activeOffset += rawhide.rawEntryToBuffer(readable, activeOffset, entryBuffer);
                return entryBuffer;
            } else if (type == LABAppendableIndex.LEAP) {
                activeOffset +=  readable.readInt(activeOffset); // entryLength
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
