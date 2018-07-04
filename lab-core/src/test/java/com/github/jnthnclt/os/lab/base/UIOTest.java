package com.github.jnthnclt.os.lab.base;

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
    }

}