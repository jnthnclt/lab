package org.colt.os.lab.guts.allocators;

/**
 *
 * @author jonathan.colt
 */
public class LABMemorySlabs {

    final byte[][] slabs;
    final int powerSize;
    final int powerLength;
    final long powerMask;

    public LABMemorySlabs(int powerSize, byte[][] slabs) {
        this.powerSize = powerSize;
        this.powerLength = 1 << powerSize;
        this.powerMask = (powerLength) - 1;
        this.slabs = slabs;
    }

    byte[] slab(long address) {
        return slabs[(int) (address >>> powerSize)];
    }

    int slabIndex(long address) {
        return (int) (address & powerMask);
    }

}
