package com.github.jnthnclt.os.lab.base;

import com.github.jnthnclt.os.lab.io.AppendableHeap;
import java.util.Arrays;
import java.util.Random;
import org.junit.Assert;
import org.testng.annotations.Test;

public class UIOTest {

    @Test
    public void testAppendableHeap() throws Exception {
        AppendableHeap appendableHeap = new AppendableHeap(1);
        Assert.assertEquals(0, appendableHeap.getFilePointer());
        Assert.assertEquals(0, appendableHeap.length());

        appendableHeap.append(new BolBuffer(new byte[] { 1 }));
        Assert.assertEquals(1, appendableHeap.getFilePointer());
        Assert.assertEquals(1, appendableHeap.length());

        appendableHeap.append(new byte[] { 1, 2, 3 }, 1, 1);
        Assert.assertEquals(2, appendableHeap.getFilePointer());
        Assert.assertEquals(2, appendableHeap.length());

        appendableHeap.appendByte((byte) 3);
        Assert.assertEquals(3, appendableHeap.getFilePointer());
        Assert.assertEquals(3, appendableHeap.length());

        appendableHeap.appendShort((short) 4);
        Assert.assertEquals(5, appendableHeap.getFilePointer());
        Assert.assertEquals(5, appendableHeap.length());

        appendableHeap.appendInt(5);
        Assert.assertEquals(9, appendableHeap.getFilePointer());
        Assert.assertEquals(9, appendableHeap.length());

        appendableHeap.appendLong(6L);
        Assert.assertEquals(17, appendableHeap.getFilePointer());
        Assert.assertEquals(17, appendableHeap.length());

        byte[] bytes = appendableHeap.getBytes();
        System.out.println(Arrays.toString(bytes));

        BolBuffer bb = new BolBuffer(bytes);
        Assert.assertEquals((byte) 1, bb.get(0));
        Assert.assertEquals((byte) 2, bb.get(1));
        Assert.assertEquals((byte) 3, bb.get(2));
        Assert.assertEquals((short) 4, bb.getShort(3));
        Assert.assertEquals(5, bb.getInt(5));
        Assert.assertEquals(6, bb.getLong(9));

        byte[] leakBytes = appendableHeap.leakBytes();
        Assert.assertTrue(leakBytes.length >= bytes.length);

        appendableHeap.reset();
        bytes = appendableHeap.getBytes();
        Assert.assertTrue(bytes.length == 0);
    }

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


        bytes = new byte[] { 0, 0, 0, 0 };
        UIO.writeBytes(new byte[] { 1, 1 }, bytes, 1);
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

    @Test
    public void miscTest() {

        Assert.assertEquals("[1, 1, 1]", IndexUtil.toString(new BolBuffer(new byte[] { 1, 1, 1 })));

        Assert.assertTrue(IndexUtil.compare(new BolBuffer(new byte[] { 1, 1, 1 }), new BolBuffer(new byte[] { 1, 1, 1 })) == 0);
        Assert.assertTrue(IndexUtil.compare(new BolBuffer(new byte[] { 1, 1, 2 }), new BolBuffer(new byte[] { 1, 1, 1 })) > 0);
        Assert.assertTrue(IndexUtil.compare(new BolBuffer(new byte[] { 1, 1, 1 }), new BolBuffer(new byte[] { 1, 1, 2 })) < 0);

        Assert.assertTrue(IndexUtil.compare(new BolBuffer(new byte[] { 1, 1, 2 }), new BolBuffer(new byte[] { 1, 1, 1 })) > 0);
        Assert.assertTrue(IndexUtil.compare(new BolBuffer(new byte[] { 1, 1, 1 }), new BolBuffer(new byte[] { 1, 1, 2 })) < 0);

        Assert.assertTrue(IndexUtil.compare(new BolBuffer(new byte[] { 1, 2, 1 }), new BolBuffer(new byte[] { 1, 1, 1 })) > 0);
        Assert.assertTrue(IndexUtil.compare(new BolBuffer(new byte[] { 1, 1, 1 }), new BolBuffer(new byte[] { 1, 2, 1 })) < 0);

        Assert.assertTrue(IndexUtil.compare(new BolBuffer(new byte[] { 2, 1, 1 }), new BolBuffer(new byte[] { 1, 1, 1 })) > 0);
        Assert.assertTrue(IndexUtil.compare(new BolBuffer(new byte[] { 1, 1, 1 }), new BolBuffer(new byte[] { 2, 1, 1 })) < 0);

        //---

        Assert.assertTrue(
            IndexUtil.compare(new BolBuffer(new byte[] { 1, 1, 1, 0, 0, 0, 0, 0, 0 }), new BolBuffer(new byte[] { 1, 1, 1, 0, 0, 0, 0, 0, 0 })) == 0);
        Assert.assertTrue(
            IndexUtil.compare(new BolBuffer(new byte[] { 1, 1, 2, 0, 0, 0, 0, 0, 0 }), new BolBuffer(new byte[] { 1, 1, 1, 0, 0, 0, 0, 0, 0 })) > 0);
        Assert.assertTrue(
            IndexUtil.compare(new BolBuffer(new byte[] { 1, 1, 1, 0, 0, 0, 0, 0, 0 }), new BolBuffer(new byte[] { 1, 1, 2, 0, 0, 0, 0, 0, 0 })) < 0);

        Assert.assertTrue(
            IndexUtil.compare(new BolBuffer(new byte[] { 1, 1, 2, 0, 0, 0, 0, 0, 0 }), new BolBuffer(new byte[] { 1, 1, 1, 0, 0, 0, 0, 0, 0 })) > 0);
        Assert.assertTrue(
            IndexUtil.compare(new BolBuffer(new byte[] { 1, 1, 1, 0, 0, 0, 0, 0, 0 }), new BolBuffer(new byte[] { 1, 1, 2, 0, 0, 0, 0, 0, 0 })) < 0);

        Assert.assertTrue(
            IndexUtil.compare(new BolBuffer(new byte[] { 1, 2, 1, 0, 0, 0, 0, 0, 0 }), new BolBuffer(new byte[] { 1, 1, 1, 0, 0, 0, 0, 0, 0 })) > 0);
        Assert.assertTrue(
            IndexUtil.compare(new BolBuffer(new byte[] { 1, 1, 1, 0, 0, 0, 0, 0, 0 }), new BolBuffer(new byte[] { 1, 2, 1, 0, 0, 0, 0, 0, 0 })) < 0);

        Assert.assertTrue(
            IndexUtil.compare(new BolBuffer(new byte[] { 2, 1, 1, 0, 0, 0, 0, 0, 0 }), new BolBuffer(new byte[] { 1, 1, 1, 0, 0, 0, 0, 0, 0 })) > 0);
        Assert.assertTrue(
            IndexUtil.compare(new BolBuffer(new byte[] { 1, 1, 1, 0, 0, 0, 0, 0, 0 }), new BolBuffer(new byte[] { 2, 1, 1, 0, 0, 0, 0, 0, 0 })) < 0);

        //---
        Assert.assertTrue(
            IndexUtil.compare(new BolBuffer(new byte[] { 0, 0, 0, 0, 0, 0, 1, 1, 1 }), new BolBuffer(new byte[] { 0, 0, 0, 0, 0, 0, 1, 1, 1 })) == 0);
        Assert.assertTrue(
            IndexUtil.compare(new BolBuffer(new byte[] { 0, 0, 0, 0, 0, 0, 1, 1, 2 }), new BolBuffer(new byte[] { 0, 0, 0, 0, 0, 0, 1, 1, 1 })) > 0);
        Assert.assertTrue(
            IndexUtil.compare(new BolBuffer(new byte[] { 0, 0, 0, 0, 0, 0, 1, 1, 1 }), new BolBuffer(new byte[] { 0, 0, 0, 0, 0, 0, 1, 1, 2 })) < 0);

        Assert.assertTrue(
            IndexUtil.compare(new BolBuffer(new byte[] { 0, 0, 0, 0, 0, 0, 1, 1, 2 }), new BolBuffer(new byte[] { 0, 0, 0, 0, 0, 0, 1, 1, 1 })) > 0);
        Assert.assertTrue(
            IndexUtil.compare(new BolBuffer(new byte[] { 0, 0, 0, 0, 0, 0, 1, 1, 1 }), new BolBuffer(new byte[] { 0, 0, 0, 0, 0, 0, 1, 1, 2 })) < 0);

        Assert.assertTrue(
            IndexUtil.compare(new BolBuffer(new byte[] { 0, 0, 0, 0, 0, 0, 1, 2, 1 }), new BolBuffer(new byte[] { 0, 0, 0, 0, 0, 0, 1, 1, 1 })) > 0);
        Assert.assertTrue(
            IndexUtil.compare(new BolBuffer(new byte[] { 0, 0, 0, 0, 0, 0, 1, 1, 1 }), new BolBuffer(new byte[] { 0, 0, 0, 0, 0, 0, 1, 2, 1 })) < 0);

        Assert.assertTrue(
            IndexUtil.compare(new BolBuffer(new byte[] { 0, 0, 0, 0, 0, 0, 2, 1, 1 }), new BolBuffer(new byte[] { 0, 0, 0, 0, 0, 0, 1, 1, 1 })) > 0);
        Assert.assertTrue(
            IndexUtil.compare(new BolBuffer(new byte[] { 0, 0, 0, 0, 0, 0, 1, 1, 1 }), new BolBuffer(new byte[] { 0, 0, 0, 0, 0, 0, 2, 1, 1 })) < 0);
    }
}