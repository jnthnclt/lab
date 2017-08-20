package org.colt.os.lab.guts.allocators;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Bytes;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import org.colt.os.lab.io.api.UIO;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class LABAppendOnlyAllocatorNGTest {

    @Test
    public void testShifting() {
        long powerSize = 10;
        int powerMask = (1 << powerSize) - 1;

        for (int a = 0; a < 2048; a += 256) {
            int address = a;
            int index = (int) (address >>> powerSize);
            address &= powerMask;

            System.out.println(index + " " + address);
        }
    }

    @Test
    public void testPowerUp1() {
        int power = 1;
        LABMemorySlabs memory = new LABMemorySlabs(power, null);
        LABMemorySlabs poweredUp = LABAppendOnlyAllocator.powerUp(memory, (allocated, reused) -> {
        });
        Assert.assertEquals(poweredUp.powerSize, memory.powerSize + power);
        Assert.assertEquals(poweredUp.slabs, null);
    }

    @Test
    public void testPowerUp2() {
        Random rand = new Random();
        int power = 3;
        byte[][] slabs = new byte[1][];
        slabs[0] = new byte[1 << power];
        rand.nextBytes(slabs[0]);

        LABMemorySlabs memory = new LABMemorySlabs(power, slabs);
        LABMemorySlabs poweredUp = LABAppendOnlyAllocator.powerUp(memory, (allocated, reused) -> {
        });
        Assert.assertEquals(poweredUp.powerSize, memory.powerSize + 1);
        Assert.assertEquals(poweredUp.slabs.length, 1);
        Assert.assertEquals(poweredUp.slabs[0], slabs[0]);
    }

    @Test
    public void testPowerUp3() {
        Random rand = new Random();
        int power = 5;
        byte[][] slabs = new byte[2][];
        slabs[0] = new byte[1 << power];
        slabs[1] = new byte[1 << power];
        rand.nextBytes(slabs[0]);
        rand.nextBytes(slabs[1]);

        LABMemorySlabs memory = new LABMemorySlabs(power, slabs);
        LABMemorySlabs poweredUp = LABAppendOnlyAllocator.powerUp(memory, (allocated, reused) -> {
        });
        Assert.assertEquals(poweredUp.powerSize, memory.powerSize + 1);
        Assert.assertEquals(poweredUp.slabs.length, 1);
        Assert.assertEquals(poweredUp.slabs[0], Bytes.concat(slabs[0], slabs[1]));
    }

    @Test
    public void testPowerUp4() {
        Random rand = new Random();
        int power = 5;
        byte[][] slabs = new byte[3][];
        slabs[0] = new byte[1 << power];
        slabs[1] = new byte[1 << power];
        slabs[2] = new byte[1 << (power - 1)];
        rand.nextBytes(slabs[0]);
        rand.nextBytes(slabs[1]);
        rand.nextBytes(slabs[2]);

        LABMemorySlabs memory = new LABMemorySlabs(power, slabs);
        LABMemorySlabs poweredUp = LABAppendOnlyAllocator.powerUp(memory, (allocated, reused) -> {
        });
        Assert.assertEquals(poweredUp.powerSize, memory.powerSize + 1);
        Assert.assertEquals(poweredUp.slabs.length, 2);
        Assert.assertEquals(poweredUp.slabs[0], Bytes.concat(slabs[0], slabs[1]));
        Assert.assertEquals(poweredUp.slabs[1], slabs[2]);
    }

    @Test(enabled = false)
    public void testReused() throws Exception {
        int power = 5;
        LABAppendOnlyAllocator allocator = new LABAppendOnlyAllocator("test", power);

        Random rand = new Random();
        byte[] bytes = new byte[1 << power];
        rand.nextBytes(bytes);

        long address = allocator.allocate(bytes, 0, bytes.length, (allocated, reused) -> {
            System.out.println("A:" + allocated + " " + reused);
            Assert.assertEquals(allocated, 64);
        });

        allocator.release(address);

        long secondAddress = allocator.allocate(bytes, 0, bytes.length, (allocated, reused) -> {
            System.out.println("B:" + allocated + " " + reused);
            Assert.assertEquals(reused, 64);
        });

        Assert.assertEquals(secondAddress, address);
    }

    @Test(invocationCount = 1)
    public void testBytes() throws Exception {

        int count = 10;
        boolean validate = true;

        int maxAllocatePower = 4;

        Map<Long, byte[]> allocated = Maps.newConcurrentMap();
        List<Long> arrayOfAllocated = Lists.newArrayListWithCapacity(count);

        long[] timeInGC = {0};
        LABAppendOnlyAllocator[] allocator = new LABAppendOnlyAllocator[1];
        Callable<Void> requestGC = () -> {
            //System.out.println("Gc");
            for (Long a : arrayOfAllocated) {
                if (validate) {
                    byte[] expected = allocated.get(a);
                    byte[] found = allocator[0].bytes(a);
                    try {
                        Assert.assertEquals(expected, found, "address:" + a + " " + Arrays.toString(expected) + " vs " + Arrays.toString(found));
                    } catch (Error e) {
                        //allocator[0].dump();
                        throw e;
                    }
                }
                long start = System.nanoTime();
                allocator[0].release(a);
                timeInGC[0] += System.nanoTime() - start;
            }
            arrayOfAllocated.clear();
            allocated.clear();
            allocator[0].freeAll();

            return null;
        };
        allocator[0] = new LABAppendOnlyAllocator("test", 2);

        // 8007104495249922497L
        Random rand = new Random();
        byte[] bytes = new byte[(int) UIO.chunkLength(maxAllocatePower)];
        rand.nextBytes(bytes);

        long elapse = 0;
        for (int i = 0; i < count; i++) {

            int l = 1 + rand.nextInt(bytes.length - 1);
            long start = System.nanoTime();
            long address = allocator[0].allocate(bytes, 0, l, (added, reused) -> {
            });
            elapse += System.nanoTime() - start;

            arrayOfAllocated.add(address);
            if (validate) {
                allocated.put(address, Arrays.copyOf(bytes, l));
            }

        }
        System.out.println("Force GC");
        requestGC.call();
        System.out.println(
            "Allocated " + count + " in " + (elapse / 1000000) + "millis gc:" + (timeInGC[0] / 1000000) + "millis total:" + ((elapse + timeInGC[0]) / 1000000));

        Runtime.getRuntime().gc();
    }

}
