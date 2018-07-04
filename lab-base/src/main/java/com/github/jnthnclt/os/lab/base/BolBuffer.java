package com.github.jnthnclt.os.lab.base;

import com.github.jnthnclt.os.lab.log.LABLogger;
import com.github.jnthnclt.os.lab.log.LABLoggerFactory;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;

/**
 * @author jonathan.colt
 */
public class BolBuffer {

    private static final LABLogger LOG = LABLoggerFactory.getLogger();

    public volatile ByteBuffer bb;
    public volatile byte[] bytes;
    public volatile int offset;
    public volatile int length = -1;

    public BolBuffer() {
    }

    public void force(ByteBuffer bb, int offset, int length) {
        this.bytes = null;
        this.bb = bb;
        this.offset = offset;
        this.length = length;
        if (offset + length > bb.limit()) {
            throw new IllegalArgumentException(bb + " cannot support offset=" + offset + " length=" + length);
        }
    }

    public void force(byte[] bytes, int offset, int length) {
        this.bb = null;
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
        if (offset + length > bytes.length) {
            throw new IllegalArgumentException(bytes.length + " cannot support offset=" + offset + " length=" + length);
        }
    }

    public BolBuffer(byte[] bytes) {
        this(bytes, 0, bytes == null ? -1 : bytes.length);
    }

    public BolBuffer(byte[] bytes, int offset, int length) {
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
    }

    public byte get(int offset) {
        try {
            if (bb != null) {
                return bb.get(this.offset + offset);
            }
            return bytes[this.offset + offset];
        } catch (Exception x) {
            LOG.error("get({}) failed against{} ", offset, this);
            throw x;
        }
    }

    public char getChar(int offset) {
        try {
            if (bb != null) {
                return bb.getChar(this.offset + offset);
            }
            return UIO.bytesChar(bytes, this.offset + offset);
        } catch (Exception x) {
            LOG.error("get({}) failed against{} ", offset, this);
            throw x;
        }
    }

    public short getShort(int offset) {
        try {
            if (bb != null) {
                return bb.getShort(this.offset + offset);
            }
            return UIO.bytesShort(bytes, this.offset + offset);
        } catch (Exception x) {
            LOG.error("getInt({}) failed against{} ", offset, this);
            throw x;
        }
    }

    public int getUnsignedShort(int offset) {
        try {
            if (bb != null) {
                return bb.getShort(this.offset + offset) & 0xffff;
            }
            return UIO.bytesUnsignedShort(bytes, this.offset + offset);
        } catch (Exception x) {
            LOG.error("getInt({}) failed against{} ", offset, this);
            throw x;
        }
    }

    public int getInt(int offset) {
        try {
            if (bb != null) {
                return bb.getInt(this.offset + offset);
            }
            return UIO.bytesInt(bytes, this.offset + offset);
        } catch (Exception x) {
            LOG.error("getInt({}) failed against{} ", offset, this);
            throw x;
        }
    }

    public long getUnsignedInt(int offset) {
        try {
            if (bb != null) {
                return bb.getInt(this.offset + offset) & 0xffffffffL;
            }
            return UIO.bytesInt(bytes, this.offset + offset) & 0xffffffffL;
        } catch (Exception x) {
            LOG.error("getInt({}) failed against{} ", offset, this);
            throw x;
        }
    }

    public long getLong(int offset) {
        try {
            if (bb != null) {
                return bb.getLong(this.offset + offset);
            }
            return UIO.bytesLong(bytes, this.offset + offset);
        } catch (Exception x) {
            LOG.error("getLong({}) failed against{} ", offset, this);
            throw x;
        }
    }

    public float getFloat(int offset) {
        try {
            if (bb != null) {
                return bb.getFloat(this.offset + offset);
            }
            return Float.intBitsToFloat(UIO.bytesInt(bytes, this.offset + offset));
        } catch (Exception x) {
            LOG.error("getLong({}) failed against{} ", offset, this);
            throw x;
        }
    }

    public double getDouble(int offset) {
        try {
            if (bb != null) {
                return bb.getDouble(this.offset + offset);
            }
            return Double.longBitsToDouble(UIO.bytesLong(bytes, this.offset + offset));
        } catch (Exception x) {
            LOG.error("getLong({}) failed against{} ", offset, this);
            throw x;
        }
    }

    public BolBuffer sliceInto(int offset, int length, BolBuffer bolBuffer) {
        if (bolBuffer == null || length == -1) {
            return null;
        }
        bolBuffer.bb = bb;
        bolBuffer.bytes = bytes;
        bolBuffer.offset = this.offset + offset;
        bolBuffer.length = length;
        return bolBuffer;

    }

    public void allocate(int length) {
        if (length < 0) {
            throw new IllegalArgumentException(" allocate must be greater that or equal to zero. length=" + length);
        }
        if (bytes == null || bytes.length < length) {
            bb = null;
            bytes = new byte[length];
        }
        this.length = length;
    }

    public byte[] copy() {
        if (length == -1) {
            return null;
        }
        byte[] copy = new byte[length];
        if (bb != null) {
            for (int i = 0; i < length; i++) { // bb you suck.
                copy[i] = bb.get(offset + i);
            }
        } else {
            System.arraycopy(bytes, offset, copy, 0, length);
        }
        return copy;
    }

    public void set(BolBuffer bolBuffer) {
        allocate(bolBuffer.length);
        offset = 0;
        length = bolBuffer.length;
        if (bolBuffer.bb != null) {
            for (int i = 0; i < bolBuffer.length; i++) {
                bytes[i] = bolBuffer.bb.get(bolBuffer.offset + i);
            }
        } else {
            System.arraycopy(bolBuffer.bytes, bolBuffer.offset, bytes, 0, length);
        }
    }

    public void set(byte[] raw) {
        bb = null;
        bytes = raw;
        offset = 0;
        length = raw.length;
    }

    public LongBuffer asLongBuffer() {
        return asByteBuffer().asLongBuffer();
    }

    public ByteBuffer asByteBuffer() {
        if (length == -1) {
            return null;
        }
        if (bb != null) {
            ByteBuffer duplicate = bb.duplicate();
            duplicate.position(offset);
            duplicate.limit(offset + length);
            return duplicate.slice();
        }
        return ByteBuffer.wrap(copy());
    }

    public void get(int offset, byte[] copyInto, int o, int l) {
        if (bb != null) {
            for (int i = 0; i < copyInto.length; i++) {
                copyInto[o + i] = bb.get(o + i);
            }
        } else {
            System.arraycopy(bytes, offset, copyInto, o, l);
        }
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("NOPE");
    }

//    public long longHashCode() {
//        return longHashCode(length);
//    }
//
//    final static long randMult = 0x5DEECE66DL;
//    final static long randAdd = 0xBL;
//    final static long randMask = (1L << 48) - 1;
//
//    public long longHashCode(long seed) {
//
//        if (bb != null) {
//            long hash = 0;
//            for (int i = 0; i < length; i++) {
//                long x = (seed * randMult + randAdd) & randMask;
//                seed = x;
//                hash += (bb.get(offset + i) + 128) * x;
//            }
//
//            return hash;
//        }
//
//        if (bytes != null) {
//            long hash = 0;
//            for (int i = 0; i < length; i++) {
//                long x = (seed * randMult + randAdd) & randMask;
//                seed = x;
//                hash += (bytes[offset + i] + 128) * x;
//            }
//            return hash;
//        }
//        return 0;
//
//    }

    final static long magic = 0xc6a4a7935bd1e995L;
    final static int prime = 47;

    public long longMurmurHashCode() {
        return longMurmurHashCode(length);
    }

    public long longMurmurHashCode(long seed) {
        if (bb != null) {
            long hash = (seed & 0xffffffffL) ^ (length * magic);

            int length8 = length >> 3;

            for (int i = 0; i < length8; i++) {
                final int i8 = offset + (i << 3);
                long word = bb.getLong(i8);

                word *= magic;
                word ^= word >>> prime;
                word *= magic;

                hash ^= word;
                hash *= magic;
            }


            int base = offset + (length & ~7);
            switch (length % 8) {
                case 7:
                    hash ^= (long) (bb.get(base + 6) & 0xff) << 48;
                case 6:
                    hash ^= (long) (bb.get(base + 5) & 0xff) << 40;
                case 5:
                    hash ^= (long) (bb.get(base + 4) & 0xff) << 32;
                case 4:
                    hash ^= (long) (bb.get(base + 3) & 0xff) << 24;
                case 3:
                    hash ^= (long) (bb.get(base + 2) & 0xff) << 16;
                case 2:
                    hash ^= (long) (bb.get(base + 1) & 0xff) << 8;
                case 1:
                    hash ^= (long) (bb.get(base) & 0xff);
                    hash *= magic;
            }

            hash ^= hash >>> prime;
            hash *= magic;
            hash ^= hash >>> prime;
            return hash;
        }

        if (bytes != null) {
            long hash = (seed & 0xffffffffL) ^ (length * magic);

            int length8 = length >> 3;

            for (int i = 0; i < length8; i++) {
                final int i8 = offset + (i << 3);
                long word = UIO.bytesLong(bytes, i8);

                word *= magic;
                word ^= word >>> prime;
                word *= magic;

                hash ^= word;
                hash *= magic;
            }

            int base = offset + (length & ~7);
            switch (length % 8) {
                case 7:
                    hash ^= (long) (bytes[base + 6] & 0xff) << 48;
                case 6:
                    hash ^= (long) (bytes[base + 5] & 0xff) << 40;
                case 5:
                    hash ^= (long) (bytes[base + 4] & 0xff) << 32;
                case 4:
                    hash ^= (long) (bytes[base + 3] & 0xff) << 24;
                case 3:
                    hash ^= (long) (bytes[base + 2] & 0xff) << 16;
                case 2:
                    hash ^= (long) (bytes[base + 1] & 0xff) << 8;
                case 1:
                    hash ^= (long) (bytes[base] & 0xff);
                    hash *= magic;
            }

            hash ^= hash >>> prime;
            hash *= magic;
            hash ^= hash >>> prime;
            return hash;
        }
        return 0;

    }

    @Override
    public String toString() {
        return "BolBuffer{" + "bb=" + bb + ", bytes=" + ((bytes == null) ? null : bytes.length) + ", offset=" + offset + ", length=" + length + '}';
    }

}
