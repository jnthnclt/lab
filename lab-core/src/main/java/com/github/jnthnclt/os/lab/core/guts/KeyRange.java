package com.github.jnthnclt.os.lab.core.guts;

import java.util.Arrays;
import java.util.Comparator;

/**
 *
 * @author jonathan.colt
 */
public class KeyRange implements Comparable<KeyRange> {

    final Comparator<byte[]> keyComparator;
    final byte[] start;
    final byte[] end;

    public KeyRange(Comparator<byte[]> keyComparator, byte[] start, byte[] end) {
        this.keyComparator = keyComparator;
        this.start = start;
        this.end = end;
    }

    boolean contains(KeyRange range) {
        if (start == null || end == null) {
            return false;
        }
        return keyComparator.compare(start, range.start) <= 0 && keyComparator.compare(end, range.end) >= 0;
    }

    @Override
    public int compareTo(KeyRange o) {

        int c = keyComparator.compare(start, o.start);
        if (c == 0) {
            c = keyComparator.compare(o.end, end); // reversed
        }
        return c;
    }

    @Override
    public String toString() {
        return "KeyRange{" + "start=" + Arrays.toString(start) + ", end=" + Arrays.toString(end) + '}';
    }

    public KeyRange join(byte[] idStart, byte[] idEnd) {
        return new KeyRange(keyComparator, min(start, idStart), max(end, idEnd));
    }

    private byte[] min(byte[] a, byte[] b) {
        return keyComparator.compare(a, b) < 0 ? a : b;
    }

    private byte[] max(byte[] a, byte[] b) {
        return keyComparator.compare(a, b) > 0 ? a : b;
    }
}
