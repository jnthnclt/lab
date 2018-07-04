package com.github.jnthnclt.os.lab.core.io;

import com.github.jnthnclt.os.lab.base.BolBuffer;
import java.io.DataInput;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public class BolBufferDataInput implements DataInput {

    private final BolBuffer buf;
    private volatile int offset = 0;

    public BolBufferDataInput(BolBuffer buf) {
        this.buf = buf;
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        buf.get(offset, b, off, len);
        offset += len;
    }

    @Override
    public int skipBytes(int n) throws IOException {
        int skippable = Math.min(n, buf.length - (buf.offset + offset));
        offset += skippable;
        return skippable;
    }

    @Override
    public boolean readBoolean() throws IOException {
        int ch = buf.get(offset) & 0xFF;
        offset++;
        return (ch != 0);
    }

    @Override
    public byte readByte() throws IOException {
        byte b = buf.get(offset);
        offset++;
        return b;
    }

    @Override
    public int readUnsignedByte() throws IOException {
        int b = buf.get(offset) & 0xFF;
        offset++;
        return b;
    }

    @Override
    public short readShort() throws IOException {
        short s = buf.getShort(offset);
        offset += 2;
        return s;
    }

    @Override
    public int readUnsignedShort() throws IOException {
        int i = buf.getShort(offset) & 0xFFFF;
        offset += 2;
        return i;
    }

    @Override
    public char readChar() throws IOException {
        char c = buf.getChar(offset);
        offset += 2;
        return c;
    }

    @Override
    public int readInt() throws IOException {
        int i = buf.getInt(offset);
        offset += 4;
        return i;
    }

    @Override
    public long readLong() throws IOException {
        long l = buf.getLong(offset);
        offset += 8;
        return l;
    }

    @Override
    public float readFloat() throws IOException {
        float f = buf.getFloat(offset);
        offset += 4;
        return f;
    }

    @Override
    public double readDouble() throws IOException {
        double d = buf.getDouble(offset);
        offset += 8;
        return d;
    }

    @Override
    public String readLine() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readUTF() throws IOException {
        throw new UnsupportedOperationException();
    }
}
