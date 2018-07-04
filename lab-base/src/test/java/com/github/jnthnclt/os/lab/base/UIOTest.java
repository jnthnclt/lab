package com.github.jnthnclt.os.lab.base;

import com.github.jnthnclt.os.lab.io.AppendableHeap;
import java.util.Random;
import org.junit.Assert;
import org.testng.annotations.Test;

public class UIOTest {
    @Test
    public void testPrimatives() throws Exception {
        Random r = new Random();
        long l = r.nextLong();
        Assert.assertEquals(UIO.bytesLong(UIO.longBytes(l)), l);

        int i = r.nextInt();
        Assert.assertEquals(UIO.bytesInt(UIO.intBytes(i)), i);

        Assert.assertEquals(UIO.bytesInt(UIO.intBytes(i, new byte[8], 4), 4), i);

        short s = (short) r.nextInt();
        Assert.assertEquals(UIO.bytesShort(UIO.shortBytes(s, new byte[2], 0)), s);

        long[] longs = new long[] { r.nextLong(), r.nextLong() };
        Assert.assertArrayEquals(UIO.bytesLongs(UIO.longsBytes(longs)), longs);

        AppendableHeap appendableHeap = new AppendableHeap(16);
        UIO.writeByteArray(appendableHeap, new byte[] { 1, 2, 3, 4 }, 1, 2, "foo");
        byte[] bytes = appendableHeap.getBytes();
        Assert.assertEquals(bytes[0], 0);
        Assert.assertEquals(bytes[1], 0);
        Assert.assertEquals(bytes[2], 0);
        Assert.assertEquals(bytes[3], 2);
        Assert.assertEquals(bytes[4], 2);
        Assert.assertEquals(bytes[5], 3);

        UIO.writeByteArray(appendableHeap, new byte[] { 7, 8 }, "foo");
        bytes = appendableHeap.getBytes();
        Assert.assertEquals(bytes[6], 0);
        Assert.assertEquals(bytes[7], 0);
        Assert.assertEquals(bytes[8], 0);
        Assert.assertEquals(bytes[9], 2);
        Assert.assertEquals(bytes[10], 7);
        Assert.assertEquals(bytes[11], 8);


        bytes = new byte[]{0,0,0,0};
        UIO.writeBytes(new byte[]{1,1}, bytes,1);
        Assert.assertEquals(bytes[0], 0);
        Assert.assertEquals(bytes[1], 1);
        Assert.assertEquals(bytes[2], 1);
        Assert.assertEquals(bytes[3], 0);
    }

    @Test
    public void iterateOnSplitsTest() {
        Iterable<byte[]> iterable = UIO.iterateOnSplits(new byte[] { 1, 1, 1, 1, 1, 1, 1, 1, 1 },
            new byte[] { 3, 3, 3, 3, 3, 3, 3, 3, 3 },
            true,
            1,
            (o1, o2) -> IndexUtil.compare(o1, 0, o1.length, o2, 0, o2.length));

        byte[][] expected = new byte[][] {
            { 1, 1, 1, 1, 1, 1, 1, 1, 1 },
            { 2, 2, 2, 2, 2, 2, 2, 2, 2 },
            { 3, 3, 3, 3, 3, 3, 3, 3, 3 }
        };
        int i = 0;
        for (byte[] bytes : iterable) {
            Assert.assertArrayEquals(expected[i], bytes);
            i++;
        }

    }

}