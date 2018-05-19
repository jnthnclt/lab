package com.github.jnthnclt.os.lab.core.guts.allocators;

import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public class LABIndexableMemory {

    private final LABAppendOnlyAllocator memoryAllocator;

    public LABIndexableMemory(LABAppendOnlyAllocator memoryAllocator) {
        this.memoryAllocator = memoryAllocator;
    }

    BolBuffer acquireBytes(long address, BolBuffer bolBuffer) {
        if (address == -1) {
            return null;
        }
        memoryAllocator.acquireBytes(address, bolBuffer);
        return bolBuffer;
    }

    public byte[] bytes(long address) {
        if (address == -1) {
            return null;
        }
        return memoryAllocator.bytes(address);
    }

    public long allocate(BolBuffer bolBuffer, LABCostChangeInBytes costInBytes) {
        if (bolBuffer == null || bolBuffer.length == -1) {
            throw new IllegalStateException();
        }
        return memoryAllocator.allocate(bolBuffer.bytes, bolBuffer.offset, bolBuffer.length, costInBytes);
    }

    public int release(long address) {
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
