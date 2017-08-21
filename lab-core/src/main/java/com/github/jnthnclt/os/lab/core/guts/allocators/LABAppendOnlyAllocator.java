package com.github.jnthnclt.os.lab.core.guts.allocators;

import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import com.github.jnthnclt.os.lab.core.io.api.UIO;
import com.github.jnthnclt.os.lab.core.util.LABLogger;
import com.github.jnthnclt.os.lab.core.util.LABLoggerFactory;

/**
 *
 * @author jonathan.colt
 */
public class LABAppendOnlyAllocator {

    private static final LABLogger LOG = LABLoggerFactory.getLogger();

    private static final int MIN_POWER = 3;
    private static final int MAX_POWER = 31;

    private final String name;
    private volatile LABMemorySlabs memory;
    private volatile long allocateNext = 0;

    public LABAppendOnlyAllocator(String name, int initialPower) {
        this.name = name;
        int power = Math.min(Math.max(MIN_POWER, initialPower), MAX_POWER);
        this.memory = new LABMemorySlabs(power, null);
    }

    public int poweredUpTo() {
        return memory.powerSize;
    }

    public boolean acquireBytes(long address, BolBuffer bolBuffer) {
        LABMemorySlabs m = memory;
        byte[] slab = m.slab(address);
        int slabAddress = m.slabIndex(address);
        bolBuffer.force(slab, slabAddress + 4, UIO.bytesInt(slab, slabAddress));
        return true;
    }

    public byte[] bytes(long address) {
        if (address == -1) {
            return null;
        }
        LABMemorySlabs m = memory;
        byte[] slab = m.slab(address);
        int slabAddress = m.slabIndex(address);

        int length = UIO.bytesInt(slab, slabAddress);
        if (length > -1) {
            byte[] copy = new byte[length];
            System.arraycopy(slab, slabAddress + 4, copy, 0, length);
            return copy;
        } else {
            throw new IllegalStateException("Address:" + slabAddress + " length=" + length);
        }

    }

    public long allocate(byte[] bytes, int offset, int length, LABCostChangeInBytes costInBytes) throws Exception {
        if (bytes == null) {
            return -1;
        }
        synchronized (this) {
            long address = allocate(4 + length, costInBytes);
            LABMemorySlabs m = memory;
            byte[] slab = m.slab(address);
            int slabAddress = m.slabIndex(address);
            UIO.intBytes(length, slab, slabAddress);
            System.arraycopy(bytes, offset, slab, slabAddress + 4, length);
            return address;
        }
    }

    private long allocate(int length, LABCostChangeInBytes costInBytes) throws Exception {

        LABMemorySlabs m = memory;
        while (length > m.powerLength && m.powerSize < MAX_POWER) {
            LOG.warn("Uping Power to {}  because of length={} for {}. Consider changing you config to {}.", m.powerSize + 1, length, name, m.powerSize + 1);
            m = powerUp(m, costInBytes);
        }
        memory = m;

        long address = allocateNext;
        int index = (int) (address >>> m.powerSize);
        int tailIndex = (int) ((address + (length - 1)) >>> m.powerSize);
        if (index == tailIndex) {
            allocateNext = address + length;
        } else {
            long desiredAddress = tailIndex * (1 << m.powerSize);
            allocateNext = desiredAddress + length;
            address = desiredAddress;
            index = (int) (desiredAddress >>> m.powerSize);
        }
        
        int indexAlignedAddress = (int) (address & m.powerMask);
        if (m.slabs == null
            || index >= m.slabs.length
            || m.slabs[index] == null
            || m.slabs[index].length < indexAlignedAddress + length) {

            if (m.slabs == null) {
                m = new LABMemorySlabs(m.powerSize, new byte[index + 1][]);
            } else if (index >= m.slabs.length) {

                byte[][] newMemory = new byte[index + 1][];
                System.arraycopy(m.slabs, 0, newMemory, 0, m.slabs.length);
                m = new LABMemorySlabs(m.powerSize, newMemory);
            }

            int power = UIO.chunkPower(indexAlignedAddress + length, MIN_POWER);
            byte[] bytes = new byte[1 << power];
            if (m.slabs[index] != null) {
                System.arraycopy(m.slabs[index], 0, bytes, 0, m.slabs[index].length);
                costInBytes.cost(bytes.length - m.slabs[index].length, 0);
            } else {
                costInBytes.cost(bytes.length, 0);
            }
            m.slabs[index] = bytes; // uck

            if (m.slabs.length == 3 && m.powerSize < MAX_POWER) { // 3
                LOG.warn("Uping Power to {} because slab count={} for {}. Consider changing you config to {}.",
                    m.powerSize + 1, m.slabs.length, name, m.powerSize + 1);
                m = powerUp(m, costInBytes);
            }
            memory = m;
        }

        return address;

    }

    static LABMemorySlabs powerUp(LABMemorySlabs m, LABCostChangeInBytes costInBytes) {
        LABMemorySlabs nm;
        int nextPowerSize = m.powerSize + 1;
        if (nextPowerSize > MAX_POWER) {
            return m;
        }
        if (m.slabs == null) {
            nm = new LABMemorySlabs(nextPowerSize, null);
        } else if (m.slabs.length == 1) {
            nm = new LABMemorySlabs(nextPowerSize, m.slabs);
        } else {
            int slabLength = m.slabs.length;
            int numSlabs = (m.slabs.length / 2) + (slabLength % 2 == 0 ? 0 : 1);
            byte[][] newSlabs = new byte[numSlabs][];
            int offset = m.powerLength;
            for (int i = 0, npi = 0; i < slabLength; i += 2, npi++) {
                if (i == slabLength - 1) {
                    newSlabs[npi] = m.slabs[i];
                } else {
                    newSlabs[npi] = new byte[1 << nextPowerSize];
                    System.arraycopy(m.slabs[i], 0, newSlabs[npi], 0, m.slabs[i].length);
                    System.arraycopy(m.slabs[i + 1], 0, newSlabs[npi], offset, m.slabs[i + 1].length);
                    costInBytes.cost(newSlabs[npi].length - (offset + m.slabs[i + 1].length), 0);
                }
            }
            nm = new LABMemorySlabs(nextPowerSize, newSlabs);
        }
        return nm;
    }

    public int release(long address) throws InterruptedException {
        if (address == -1) {
            return 0;
        }
        LABMemorySlabs m = memory;
        byte[] slab = m.slab(address);
        int slabAddress = m.slabIndex(address);
        return UIO.bytesInt(slab, slabAddress);
    }

    public int compareLB(Rawhide rawhide, long leftAddress, byte[] rightBytes, int rightOffset, int rightLength
    ) {
        if (leftAddress == -1) {
            return rawhide.compareBB(null, -1, -1, rightBytes, rightOffset, rightBytes == null ? -1 : rightLength);
        } else {
            LABMemorySlabs m = memory;
            int leftIndex = (int) (leftAddress >>> m.powerSize);
            leftAddress &= m.powerMask;
            byte[] leftCopy = m.slabs[leftIndex];

            int leftLength = UIO.bytesInt(leftCopy, (int) leftAddress);
            return rawhide.compareBB(leftCopy, (int) leftAddress + 4, leftLength, rightBytes, rightOffset, rightBytes == null ? -1 : rightLength);
        }
    }

    public int compareBL(Rawhide rawhide, byte[] leftBytes, int leftOffset, int leftLength, long rightAddress
    ) {
        if (rightAddress == -1) {
            return rawhide.compareBB(leftBytes, leftOffset, leftBytes == null ? -1 : leftLength, null, -1, -1);
        } else {
            LABMemorySlabs m = memory;
            int rightIndex = (int) (rightAddress >>> m.powerSize);
            rightAddress &= m.powerMask;
            byte[] rightCopy = m.slabs[rightIndex];

            int l2 = UIO.bytesInt(rightCopy, (int) rightAddress);
            return rawhide.compareBB(leftBytes, leftOffset, leftBytes == null ? -1 : leftLength, rightCopy, (int) rightAddress + 4, l2);
        }
    }

    public int compareBB(Rawhide rawhide, byte[] leftBytes, int leftOffset, int leftLength, byte[] rightBytes, int rightOffset, int rightLength
    ) {
        return rawhide.compareBB(leftBytes, leftOffset, leftBytes == null ? -1 : leftLength, rightBytes, rightOffset, rightBytes == null ? -1 : rightLength);
    }

    public void freeAll() {

    }
}
