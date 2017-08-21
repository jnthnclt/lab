package com.github.jnthnclt.os.lab.core.guts.allocators;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class LABMemorySlabsNGTest {

    @Test
    public void testSomeMethod() {
        byte[] slabA = new byte[1 << 4];
        int v = 0;
        for (int i = 0; i < slabA.length; i++) {
            slabA[i] = (byte) v;
            v++;
        }
        byte[] slabB = new byte[1 << 4];
        for (int i = 0; i < slabB.length; i++) {
            slabB[i] = (byte) v;
            v++;
        }

        byte[] slabC = new byte[1 << 4];
        for (int i = 0; i < slabC.length; i++) {
            slabC[i] = (byte) v;
            v++;
        }
        byte[] slabD = new byte[1 << 2];
        for (int i = 0; i < slabD.length; i++) {
            slabD[i] = (byte) v;
            v++;
        }
        LABMemorySlabs memorySlabs = new LABMemorySlabs(4, new byte[][]{slabA, slabB, slabC, slabD});

        for (int a = 0; a < v; a++) {
            byte[] slab = memorySlabs.slab(a);
            int slabIndex = memorySlabs.slabIndex(a);
            Assert.assertEquals(slab[slabIndex], a);
        }
    }

}
