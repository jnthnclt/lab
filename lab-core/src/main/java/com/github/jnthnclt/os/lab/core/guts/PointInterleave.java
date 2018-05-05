package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.core.api.FormatTransformer;
import com.github.jnthnclt.os.lab.core.guts.api.Next;
import com.github.jnthnclt.os.lab.core.guts.api.Scanner;
import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.guts.api.RawEntryStream;
import com.github.jnthnclt.os.lab.core.guts.api.ReadIndex;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;

public class PointInterleave implements Scanner, RawEntryStream {

    private final Rawhide rawhide;
    private FormatTransformer nextReadKeyFormatTransformer;
    private FormatTransformer nextReadValueFormatTransformer;
    private boolean once;
    private BolBuffer nextRawEntry;

    public PointInterleave(ReadIndex[] indexs, byte[] key, Rawhide rawhide, boolean hashIndexEnabled) throws Exception {
        this.rawhide = rawhide;
        BolBuffer entryKeyBuffer = new BolBuffer();
        for (ReadIndex index : indexs) {
            Scanner scanner = null;
            try {
                BolBuffer entryBuffer = new BolBuffer(); // must be new since we retain a reference
                scanner = index.pointScan(new ActiveScanPoint(hashIndexEnabled), key, entryBuffer, entryKeyBuffer);
                if (scanner != null) {
                    scanner.next(this, null);
                    scanner.close();
                }
                if (!rawhide.hasTimestampVersion() && nextRawEntry != null) {
                    // this rawhide doesn't support timestamps, so as soon as we find one, we win!
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

    @Override
    public Next next(RawEntryStream stream,  BolBuffer nextHint) throws Exception {
        if (once) {
            return Next.stopped;
        }
        stream.stream(nextReadKeyFormatTransformer, nextReadValueFormatTransformer, nextRawEntry);
        once = true;
        return Next.more;
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public boolean stream(FormatTransformer readKeyFormatTransformer, FormatTransformer readValueFormatTransformer, BolBuffer rawEntry) throws Exception {
        if (nextRawEntry != null) {
            long leftTimestamp = rawhide.timestamp(nextReadKeyFormatTransformer, nextReadValueFormatTransformer, nextRawEntry);
            long rightTimestamp = rawhide.timestamp(readKeyFormatTransformer, readValueFormatTransformer, rawEntry);
            if (leftTimestamp > rightTimestamp) {
                return true;
            } else if (leftTimestamp == rightTimestamp) {
                long leftVersion = rawhide.version(nextReadKeyFormatTransformer, nextReadValueFormatTransformer, nextRawEntry);
                long rightVersion = rawhide.version(readKeyFormatTransformer, readValueFormatTransformer, rawEntry);
                if (leftVersion >= rightVersion) {
                    return true;
                }
            }
        }
        nextRawEntry = rawEntry;
        nextReadKeyFormatTransformer = readKeyFormatTransformer;
        nextReadValueFormatTransformer = readValueFormatTransformer;
        return true;
    }
}
