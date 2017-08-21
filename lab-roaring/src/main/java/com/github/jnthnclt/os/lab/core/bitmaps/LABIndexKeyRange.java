package com.github.jnthnclt.os.lab.core.bitmaps;

import com.google.common.primitives.UnsignedBytes;
import java.util.Comparator;

public class LABIndexKeyRange {

    static final Comparator<byte[]> lexicographicalComparator = UnsignedBytes.lexicographicalComparator();
    private final byte[] startInclusiveKey;
    private final byte[] stopExclusiveKey;

    public LABIndexKeyRange(byte[] startInclusiveKey, byte[] stopExclusiveKey) {
        this.startInclusiveKey = startInclusiveKey;
        this.stopExclusiveKey = stopExclusiveKey;
    }

    public byte[] getStartInclusiveKey() {
        return startInclusiveKey;
    }

    public byte[] getStopExclusiveKey() {
        return stopExclusiveKey;
    }

    public boolean contains(byte[] key) {
        int compare = lexicographicalComparator.compare(startInclusiveKey, key);
        if (compare <= 0) {
            return lexicographicalComparator.compare(key, stopExclusiveKey) < 0;
        }
        return false;
    }

}
