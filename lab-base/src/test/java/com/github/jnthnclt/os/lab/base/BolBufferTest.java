package com.github.jnthnclt.os.lab.base;

import java.nio.ByteBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BolBufferTest {
    @Test
    public void testForce() throws Exception {
        BolBuffer bolBuffer = new BolBuffer();
        bolBuffer.force(new byte[] { 1, 2, 3 }, 1, 2);
        Assert.assertEquals(bolBuffer.length, 2);
        Assert.assertEquals(bolBuffer.get(0), 2);

        Assert.assertEquals(bolBuffer.toString(), "BolBuffer{bb=null, bytes=3, offset=1, length=2}");

        bolBuffer.force(ByteBuffer.wrap(new byte[] { 1, 2, 3 }), 1, 2);
        Assert.assertEquals(bolBuffer.length, 2);
        Assert.assertEquals(bolBuffer.get(0), 2);
    }

    @Test
    public void testGetInt() throws Exception {
        BolBuffer bolBuffer = new BolBuffer();
        bolBuffer.force(UIO.intBytes(1234), 0, 4);
        Assert.assertEquals(bolBuffer.getInt(0), 1234);

        bolBuffer.force(ByteBuffer.wrap(UIO.intBytes(1234)), 0, 4);
        Assert.assertEquals(bolBuffer.getInt(0), 1234);
    }

    @Test
    public void testGetUnsignedInt() throws Exception {
    }

    @Test
    public void testGetLong() throws Exception {
        BolBuffer bolBuffer = new BolBuffer();
        bolBuffer.force(UIO.longBytes(Integer.MAX_VALUE * 2L), 0, 8);
        Assert.assertEquals(bolBuffer.getLong(0), Integer.MAX_VALUE * 2L);

        bolBuffer.force(ByteBuffer.wrap(UIO.longBytes(Integer.MAX_VALUE * 2L)), 0, 8);
        Assert.assertEquals(bolBuffer.getLong(0), Integer.MAX_VALUE * 2L);
    }

    @Test
    public void testGetFloat() throws Exception {
        BolBuffer bolBuffer = new BolBuffer();
        bolBuffer.force(UIO.intBytes(Float.floatToIntBits(0.1234f)), 0, 4);
        Assert.assertEquals(Float.intBitsToFloat(bolBuffer.getInt(0)), 0.1234f);
        Assert.assertEquals(bolBuffer.getFloat(0),0.1234f);

        bolBuffer.force(ByteBuffer.wrap(UIO.intBytes(Float.floatToIntBits(0.1234f))), 0, 4);
        Assert.assertEquals(Float.intBitsToFloat(bolBuffer.getInt(0)), 0.1234f);
        Assert.assertEquals(bolBuffer.getFloat(0),0.1234f);
    }

    @Test
    public void testGetDouble() throws Exception {
        BolBuffer bolBuffer = new BolBuffer();
        bolBuffer.force(UIO.longBytes(Double.doubleToLongBits(Float.MAX_VALUE * 2d)), 0, 8);
        Assert.assertEquals(Double.longBitsToDouble(bolBuffer.getLong(0)), Float.MAX_VALUE * 2d);
        Assert.assertEquals(bolBuffer.getDouble(0),Float.MAX_VALUE * 2d);

        bolBuffer.force(ByteBuffer.wrap(UIO.longBytes(Double.doubleToLongBits(Float.MAX_VALUE * 2d))), 0, 8);
        Assert.assertEquals(Double.longBitsToDouble(bolBuffer.getLong(0)), Float.MAX_VALUE * 2d);
        Assert.assertEquals(bolBuffer.getDouble(0),Float.MAX_VALUE * 2d);
    }

    @Test
    public void testLongMurmurHashCode() throws Exception {
        BolBuffer bolBuffer = new BolBuffer();
        bolBuffer.set(new byte[]{1,2,3,4,5,6,7});
        System.out.println(bolBuffer.longMurmurHashCode());
        bolBuffer.set(new byte[]{1,2,3,4,5,6,7,8,9});
        System.out.println(bolBuffer.longMurmurHashCode());

        bolBuffer.force(ByteBuffer.wrap(new byte[]{1,2,3,4,5,6,7}), 0, 7);
        System.out.println(bolBuffer.longMurmurHashCode());
        bolBuffer.force(ByteBuffer.wrap(new byte[]{1,2,3,4,5,6,7,8,9}), 0, 9);
        System.out.println(bolBuffer.longMurmurHashCode());
    }



}