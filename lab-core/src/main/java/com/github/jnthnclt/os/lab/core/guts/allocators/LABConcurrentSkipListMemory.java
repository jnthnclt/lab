package com.github.jnthnclt.os.lab.core.guts.allocators;

import com.github.jnthnclt.os.lab.base.BolBuffer;
import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;

/**
 *
 * @author jonathan.colt
 */
public class LABConcurrentSkipListMemory {

    private final Rawhide rawhide;
    private final LABIndexableMemory indexableMemory;

    public LABConcurrentSkipListMemory(Rawhide rawhide, LABIndexableMemory indexableMemory) {

        this.rawhide = rawhide;
        this.indexableMemory = indexableMemory;
    }

    public byte[] bytes(long chunkAddress) {
        return indexableMemory.bytes(chunkAddress);
    }

    public BolBuffer acquireBytes(long chunkAddress, BolBuffer bolBuffer) {
        return indexableMemory.acquireBytes(chunkAddress, bolBuffer);
    }

    public long allocate(BolBuffer bytes, LABCostChangeInBytes costInBytes) {
        return indexableMemory.allocate(bytes, costInBytes);
    }

    public int release(long address) {
        return indexableMemory.release(address);
    }

    public int compareLB(long left, byte[] right, int rightOffset, int rightLength) {
        return indexableMemory.compareLB(rawhide, left, right, rightOffset, rightLength);
    }

    public int compareBL(byte[] left, int leftOffset, int leftLength, long right) {
        return indexableMemory.compareBL(rawhide, left, leftOffset, leftLength, right);
    }

    public int compareBB(byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength) {
        return indexableMemory.compareBB(rawhide, left, leftOffset, leftLength, right, rightOffset, rightLength);
    }

    int poweredUpTo() {
        return indexableMemory.poweredUpTo();
    }

}
