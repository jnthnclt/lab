package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.guts.api.ReadIndex;
import com.github.jnthnclt.os.lab.core.guts.api.Scanner;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;

public class PointInterleave implements Scanner {

    private final Rawhide rawhide;
    private BolBuffer nextRawEntry;

    public PointInterleave(ReadIndex[] indexs, byte[] key, Rawhide rawhide, boolean hashIndexEnabled) throws Exception {
        this.rawhide = rawhide;
        BolBuffer entryKeyBuffer = new BolBuffer();
        for (ReadIndex index : indexs) {
            Scanner scanner = null;
            try {
                BolBuffer entryBuffer = new BolBuffer(); // must be new since we retain a reference
                scanner = index.pointScan(hashIndexEnabled, key);
                if (scanner != null) {
                    add(scanner.next(entryBuffer, null));
                    scanner.close();
                }
                if (!rawhide.hasTimestampVersion() && nextRawEntry != null) {
                    break;
                }
            } catch (Throwable t) {
                if (scanner != null) {
                    scanner.close();
                }
                throw t;
            }
        }
    }

    private void add(BolBuffer rawEntry) throws Exception {
        if (nextRawEntry != null) {
            long leftTimestamp = rawhide.timestamp(nextRawEntry);
            long rightTimestamp = rawhide.timestamp(rawEntry);
            if (leftTimestamp > rightTimestamp) {
                return;
            } else if (leftTimestamp == rightTimestamp) {
                long leftVersion = rawhide.version(nextRawEntry);
                long rightVersion = rawhide.version(rawEntry);
                if (leftVersion >= rightVersion) {
                    return;
                }
            }
        }
        nextRawEntry = rawEntry;
    }

    @Override
    public BolBuffer next(BolBuffer rawEntry,  BolBuffer nextHint) throws Exception {
        try {
            return nextRawEntry;
        } finally {
            nextRawEntry = null;
        }
    }

    @Override
    public void close() throws Exception {

    }


}
