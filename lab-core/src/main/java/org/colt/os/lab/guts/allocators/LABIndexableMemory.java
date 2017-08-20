package org.colt.os.lab.guts.allocators;

import org.colt.os.lab.api.rawhide.Rawhide;
import org.colt.os.lab.io.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public class LABIndexableMemory {

    private final LABAppendOnlyAllocator memoryAllocator;

    public LABIndexableMemory(LABAppendOnlyAllocator memoryAllocator) {
        this.memoryAllocator = memoryAllocator;
    }

    BolBuffer acquireBytes(long address, BolBuffer bolBuffer) throws Exception {
        if (address == -1) {
            return null;
        }
        memoryAllocator.acquireBytes(address, bolBuffer);
        return bolBuffer;
    }

    public byte[] bytes(long address) throws InterruptedException {
        if (address == -1) {
            return null;
        }
        return memoryAllocator.bytes(address);
    }

    public long allocate(BolBuffer bolBuffer, LABCostChangeInBytes costInBytes) throws Exception {
        if (bolBuffer == null || bolBuffer.length == -1) {
            throw new IllegalStateException();
        }
        return memoryAllocator.allocate(bolBuffer.bytes, bolBuffer.offset, bolBuffer.length, costInBytes);
    }

    public int release(long address) throws Exception {
        if (address == -1) {
            return 0;
        }
        return memoryAllocator.release(address);
    }

    public int compareLB(Rawhide rawhide, long left, byte[] right, int rightOffset, int rightLength) {
        return memoryAllocator.compareLB(rawhide, left, right, rightOffset, rightLength);
    }

    public int compareBL(Rawhide rawhide, byte[] left, int leftOffset, int leftLength, long right) {
        return memoryAllocator.compareBL(rawhide, left, leftOffset, leftLength, right);
    }

    public int compareBB(Rawhide rawhide, byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength) {
        return memoryAllocator.compareBB(rawhide, left, leftOffset, leftLength, right, rightOffset, rightLength);
    }

    int poweredUpTo() {
        return memoryAllocator.poweredUpTo();
    }

}
