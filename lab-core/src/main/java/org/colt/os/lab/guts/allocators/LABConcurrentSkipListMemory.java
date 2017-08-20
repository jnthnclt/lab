package org.colt.os.lab.guts.allocators;

import org.colt.os.lab.api.rawhide.Rawhide;
import org.colt.os.lab.io.BolBuffer;

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

    public byte[] bytes(long chunkAddress) throws InterruptedException {
        return indexableMemory.bytes(chunkAddress);
    }

    public BolBuffer acquireBytes(long chunkAddress, BolBuffer bolBuffer) throws Exception {
        return indexableMemory.acquireBytes(chunkAddress, bolBuffer);
    }

    public long allocate(BolBuffer bytes, LABCostChangeInBytes costInBytes) throws Exception {
        return indexableMemory.allocate(bytes, costInBytes);
    }

    public int release(long address) throws Exception {
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
