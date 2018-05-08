package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.guts.api.ReadIndex;
import com.github.jnthnclt.os.lab.core.guts.api.Scanner;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;

public class PointInterleave {

    public static BolBuffer get(ReadIndex[] indexs, byte[] key, Rawhide rawhide, boolean hashIndexEnabled) throws Exception {
        BolBuffer nextRawEntry = null;
        for (ReadIndex index : indexs) {
            Scanner scanner = null;
            try {
                BolBuffer entryBuffer = new BolBuffer(); // must be new since we retain a reference
                scanner = index.pointScan(hashIndexEnabled, key);
                if (scanner != null) {
                    nextRawEntry = pick(rawhide, nextRawEntry, scanner.next(entryBuffer, null));
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
        return nextRawEntry;
    }

    private static BolBuffer pick( Rawhide rawhide, BolBuffer nextRawEntry, BolBuffer rawEntry) throws Exception {
        if (nextRawEntry != null) {
            long leftTimestamp = rawhide.timestamp(nextRawEntry);
            long rightTimestamp = rawhide.timestamp(rawEntry);
            if (leftTimestamp > rightTimestamp) {
                return nextRawEntry;
            } else if (leftTimestamp == rightTimestamp) {
                long leftVersion = rawhide.version(nextRawEntry);
                long rightVersion = rawhide.version(rawEntry);
                if (leftVersion >= rightVersion) {
                    return nextRawEntry;
                }
            }
        }
        return rawEntry;
    }
}
