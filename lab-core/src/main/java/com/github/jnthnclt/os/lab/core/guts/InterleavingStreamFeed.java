package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.core.api.FormatTransformer;
import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.guts.api.Next;
import com.github.jnthnclt.os.lab.core.guts.api.RawEntryStream;
import com.github.jnthnclt.os.lab.core.guts.api.Scanner;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;

class InterleavingStreamFeed implements Comparable<InterleavingStreamFeed>, RawEntryStream {

    private final int index;
    private final Scanner scanner;
    private final Rawhide rawhide;

    BolBuffer entryKeyBuffer = new BolBuffer();
    FormatTransformer nextReadKeyFormatTransformer;
    FormatTransformer nextReadValueFormatTransformer;
    BolBuffer nextRawEntry;

    public InterleavingStreamFeed(int index, Scanner scanner, Rawhide rawhide) {
        this.index = index;
        this.scanner = scanner;
        this.rawhide = rawhide;
    }

    public BolBuffer feedNext() throws Exception {
        Next hadNext = scanner.next(this, null);
        if (hadNext != Next.more) {
            nextRawEntry = null;
        }
        return nextRawEntry;
    }

    @Override
    public boolean stream(FormatTransformer readKeyFormatTransformer, FormatTransformer readValueFormatTransformer, BolBuffer rawEntry) throws Exception {
        nextRawEntry = rawEntry;
        nextReadKeyFormatTransformer = readKeyFormatTransformer;
        nextReadValueFormatTransformer = readValueFormatTransformer;
        return true;
    }

    @Override
    public int compareTo(InterleavingStreamFeed o) {
        try {
            int c = rawhide.mergeCompare(nextReadKeyFormatTransformer,
                nextReadValueFormatTransformer,
                nextRawEntry,
                entryKeyBuffer,
                o.nextReadKeyFormatTransformer,
                o.nextReadValueFormatTransformer,
                o.nextRawEntry,
                o.entryKeyBuffer);

            if (c != 0) {
                return c;
            }
            c = Integer.compare(index, o.index);
            return c;
        } catch (Exception x) {
            throw new RuntimeException(x);
        }
    }

    public void close() throws Exception {
        scanner.close();
    }
}
