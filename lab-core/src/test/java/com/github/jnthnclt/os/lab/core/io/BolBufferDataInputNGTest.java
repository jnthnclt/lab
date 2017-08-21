package com.github.jnthnclt.os.lab.core.io;

import com.github.jnthnclt.os.lab.core.io.api.UIO;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class BolBufferDataInputNGTest {

    @Test
    public void testReadFully_byteArr() throws Exception {
        byte[] bytes = new byte[4 + 10 + 7];
        for (int i = 0; i < 10; i++) {
            bytes[4 + i] = (byte) i;
        }
        BolBufferDataInput dataInput = new BolBufferDataInput(new BolBuffer(bytes));
        dataInput.skipBytes(4);

        byte[] read = new byte[10];
        dataInput.readFully(read, 0, 10);
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(read[i], bytes[4 + i]);
        }

    }

    @Test
    public void testReadBoolean() throws Exception {
        byte[] bytes = new byte[10];
        for (int i = 0; i < 10; i++) {
            bytes[i] = (byte) (i % 2);
        }
        BolBufferDataInput dataInput = new BolBufferDataInput(new BolBuffer(bytes));

        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(dataInput.readBoolean(), (i % 2 == 0) ? false : true);
        }
    }

    @Test
    public void testReadByte() throws Exception {
        byte[] bytes = new byte[256];
        for (int i = 0; i < 256; i++) {
            bytes[i] = (byte) i;
        }
        BolBufferDataInput dataInput = new BolBufferDataInput(new BolBuffer(bytes));
        for (int i = 0; i < 256; i++) {
            Assert.assertEquals(dataInput.readByte(), (byte) i);
        }
    }

    @Test
    public void testReadUnsignedByte() throws Exception {
        byte[] bytes = new byte[256];
        for (int i = 0; i < 256; i++) {
            bytes[i] = (byte) i;
        }
        BolBufferDataInput dataInput = new BolBufferDataInput(new BolBuffer(bytes));
        for (int i = 0; i < 256; i++) {
            Assert.assertEquals(dataInput.readUnsignedByte(), i);
        }
    }

    @Test
    public void testReadShort() throws Exception {
        byte[] bytes = new byte[16 * 2];
        for (int i = 0; i < 16; i++) {
            UIO.shortBytes((short) Math.pow(2, i), bytes, i * 2);
        }
        BolBufferDataInput dataInput = new BolBufferDataInput(new BolBuffer(bytes));
        for (int i = 0; i < 16; i++) {
            Assert.assertEquals(dataInput.readShort(), (short) Math.pow(2, i));
        }
    }

    @Test
    public void testReadUnsignedShort() throws Exception {
        byte[] bytes = new byte[16 * 2];
        for (int i = 0; i < 16; i++) {
            UIO.shortBytes((short) Math.pow(2, i), bytes, i * 2);
        }
        BolBufferDataInput dataInput = new BolBufferDataInput(new BolBuffer(bytes));
        for (int i = 0; i < 16; i++) {
            Assert.assertEquals(dataInput.readUnsignedShort(), (int) Math.pow(2, i));
        }
    }

    @Test
    public void testReadChar() throws Exception {
        byte[] bytes = new byte[16 * 2];
        for (int i = 0; i < 16; i++) {
            UIO.shortBytes((short) Math.pow(2, i), bytes, i * 2);
        }
        BolBufferDataInput dataInput = new BolBufferDataInput(new BolBuffer(bytes));
        for (int i = 0; i < 16; i++) {
            Assert.assertEquals(dataInput.readChar(), (char) Math.pow(2, i));
        }
    }

    @Test
    public void testReadInt() throws Exception {
        byte[] bytes = new byte[32 * 4];
        for (int i = 0; i < 32; i++) {
            UIO.intBytes((int) Math.pow(2, i), bytes, i * 4);
        }
        BolBufferDataInput dataInput = new BolBufferDataInput(new BolBuffer(bytes));
        for (int i = 0; i < 16; i++) {
            Assert.assertEquals(dataInput.readInt(), (int) Math.pow(2, i));
        }
    }

    @Test
    public void testReadLong() throws Exception {
        byte[] bytes = new byte[64 * 8];
        for (int i = 0; i < 64; i++) {
            UIO.longBytes((long) Math.pow(2, i), bytes, i * 8);
        }
        BolBufferDataInput dataInput = new BolBufferDataInput(new BolBuffer(bytes));
        for (int i = 0; i < 16; i++) {
            Assert.assertEquals(dataInput.readLong(), (long) Math.pow(2, i));
        }
    }

}
