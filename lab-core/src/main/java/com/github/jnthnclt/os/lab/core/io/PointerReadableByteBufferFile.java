package com.github.jnthnclt.os.lab.core.io;

import com.github.jnthnclt.os.lab.base.BolBuffer;
import com.github.jnthnclt.os.lab.base.UIO;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

/**
 *
 */
public class PointerReadableByteBufferFile {


    public static final long MAX_BUFFER_SEGMENT_SIZE = UIO.chunkLength(30);
    public static final long MAX_POSITION = MAX_BUFFER_SEGMENT_SIZE * 100;

    private final long maxBufferSegmentSize;
    private final File file;
    private final boolean writeable;
    private final long length;

    private final ByteBuffer[] bbs;

    private final int fShift;
    private final long fseekMask;

    public PointerReadableByteBufferFile(long maxBufferSegmentSize,
        File file, boolean writeable) throws IOException {

        this.maxBufferSegmentSize = Math.min(UIO.chunkLength(UIO.chunkPower(maxBufferSegmentSize, 0)), MAX_BUFFER_SEGMENT_SIZE);

        this.file = file;
        this.writeable = writeable;

        // test power of 2
        if ((this.maxBufferSegmentSize & (this.maxBufferSegmentSize - 1)) == 0) {
            this.fShift = Long.numberOfTrailingZeros(this.maxBufferSegmentSize);
            this.fseekMask = this.maxBufferSegmentSize - 1;
        } else {
            throw new IllegalArgumentException("It's hard to ensure powers of 2");
        }
        this.length = file.length();
        long position = this.length;
        int filerIndex = (int) (position >> fShift);
        long filerSeek = position & fseekMask;

        int newLength = filerIndex + 1;
        ByteBuffer[] newFilers = new ByteBuffer[newLength];
        for (int n = 0; n < newLength; n++) {
            if (n < newLength - 1) {
                newFilers[n] = allocate(n, maxBufferSegmentSize);
            } else {
                newFilers[n] = allocate(n, filerSeek);
            }
        }
        bbs = newFilers;
    }

    public long length() {
        return length;
    }

    private ByteBuffer allocate(int index, long length) throws IOException {
        long segmentOffset = maxBufferSegmentSize * index;
        long requiredLength = segmentOffset + length;
        try (RandomAccessFile raf = new RandomAccessFile(file, writeable ? "rw" : "r")) {
            if (requiredLength > raf.length()) {
                raf.seek(requiredLength - 1);
                raf.write(0);
            }
            raf.seek(segmentOffset);
            try (FileChannel channel = raf.getChannel()) {
                return channel.map(writeable ? FileChannel.MapMode.READ_WRITE : MapMode.READ_ONLY,
                    segmentOffset, Math.min(maxBufferSegmentSize, channel.size() - segmentOffset));
            }
        }
    }

    private int read(int bbIndex, int bbSeek) {
        if (!hasRemaining(bbIndex, bbSeek, 1)) {
            return -1;
        }
        byte b = bbs[bbIndex].get(bbSeek);
        return b & 0xFF;
    }

    public int read(long position) throws IOException {
        int bbIndex = (int) (position >> fShift);
        int bbSeek = (int) (position & fseekMask);

        int read = read(bbIndex, bbSeek);
        while (read == -1 && bbIndex < bbs.length - 1) {
            bbIndex++;
            read = read(bbIndex, 0);
        }
        return read;
    }

    private int readAtleastOne(long position) throws IOException {
        int bbIndex = (int) (position >> fShift);
        int bbSeek = (int) (position & fseekMask);
        int r = read(bbIndex, bbSeek);
        if (r == -1) {
            throw new EOFException("Failed to fully read 1 byte");
        }
        return r;
    }

    private boolean hasRemaining(int bbIndex, int bbSeek, int length) {
        return bbs[bbIndex].limit() - bbSeek >= length;
    }

    public int readInt(long position) throws IOException {
        int bbIndex = (int) (position >> fShift);
        int bbSeek = (int) (position & fseekMask);

        if (hasRemaining(bbIndex, bbSeek, 4)) {
            return bbs[bbIndex].getInt(bbSeek);
        } else {
            int b0 = readAtleastOne(position);
            int b1 = readAtleastOne(position + 1);
            int b2 = readAtleastOne(position + 2);
            int b3 = readAtleastOne(position + 3);

            int v = 0;
            v |= (b0 & 0xFF);
            v <<= 8;
            v |= (b1 & 0xFF);
            v <<= 8;
            v |= (b2 & 0xFF);
            v <<= 8;
            v |= (b3 & 0xFF);
            return v;
        }
    }

    public long readLong(long position) throws IOException {
        int bbIndex = (int) (position >> fShift);
        int bbSeek = (int) (position & fseekMask);

        if (hasRemaining(bbIndex, bbSeek, 8)) {
            return bbs[bbIndex].getLong(bbSeek);
        } else {
            int b0 = readAtleastOne(position);
            int b1 = readAtleastOne(position + 1);
            int b2 = readAtleastOne(position + 2);
            int b3 = readAtleastOne(position + 3);
            int b4 = readAtleastOne(position + 4);
            int b5 = readAtleastOne(position + 5);
            int b6 = readAtleastOne(position + 6);
            int b7 = readAtleastOne(position + 7);

            long v = 0;
            v |= (b0 & 0xFF);
            v <<= 8;
            v |= (b1 & 0xFF);
            v <<= 8;
            v |= (b2 & 0xFF);
            v <<= 8;
            v |= (b3 & 0xFF);
            v <<= 8;
            v |= (b4 & 0xFF);
            v <<= 8;
            v |= (b5 & 0xFF);
            v <<= 8;
            v |= (b6 & 0xFF);
            v <<= 8;
            v |= (b7 & 0xFF);

            return v;
        }
    }

    private int read(int bbIndex, int bbSeek, byte[] b, int _offset, int _len) {
        ByteBuffer bb = bbs[bbIndex];
        int remaining = bb.limit() - bbSeek;
        if (remaining <= 0) {
            return -1;
        }
        int count = Math.min(_len, remaining);
        for (int i = 0; i < count; i++) {
            b[_offset + i] = bb.get(bbSeek + i);
        }
        return count;
    }

    public int read(long position, byte[] b, int offset, int len) throws IOException {
        if (len == 0) {
            return 0;
        }

        int bbIndex = (int) (position >> fShift);
        int bbSeek = (int) (position & fseekMask);

        int remaining = len;
        int read = read(bbIndex, bbSeek, b, offset, remaining);
        if (read == -1) {
            read = 0;
        }
        offset += read;
        remaining -= read;
        while (remaining > 0 && bbIndex < bbs.length - 1) {
            bbIndex++;
            bbSeek = 0;
            read = read(bbIndex, bbSeek, b, offset, remaining);
            if (read == -1) {
                read = 0;
            }
            offset += read;
            remaining -= read;
        }
        if (len == remaining) {
            return -1;
        }
        return offset;
    }

    public BolBuffer sliceIntoBuffer(long offset, int length, BolBuffer entryBuffer) throws IOException {

        int bbIndex = (int) (offset >> fShift);
        if (bbIndex == (int) (offset + length >> fShift)) {
            int filerSeek = (int) (offset & fseekMask);
            entryBuffer.force(bbs[bbIndex], filerSeek, length);
        } else {
            byte[] rawEntry = new byte[length]; // very rare only on bb boundaries
            read(offset, rawEntry, 0, length);
            entryBuffer.force(rawEntry, 0, length);
        }
        return entryBuffer;
    }

    public void close() {
        if (bbs.length > 0) {
            ByteBuffer bb = bbs[0];
            if (bb != null) {
                DirectBufferCleaner.clean(bb);
            }
        }
    }


    public void write(long position, byte b) {
        int bbIndex = (int) (position >> fShift);
        int bbSeek = (int) (position & fseekMask);
        bbs[bbIndex].put(bbSeek, b);
    }

    public void writeShort(long position, short v) throws IOException {
        int bbIndex = (int) (position >> fShift);
        int bbSeek = (int) (position & fseekMask);

        if (hasRemaining(bbIndex, bbSeek, 2)) {
            bbs[bbIndex].putShort(bbSeek, v);
        } else {
            write(position, (byte) (v >>> 8));
            write(position + 1, (byte) (v));
        }
    }

    public void writeInt(long position, int v) throws IOException {
        int bbIndex = (int) (position >> fShift);
        int bbSeek = (int) (position & fseekMask);

        if (hasRemaining(bbIndex, bbSeek, 4)) {
            bbs[bbIndex].putInt(bbSeek, v);
        } else {
            write(position, (byte) (v >>> 24));
            write(position + 1, (byte) (v >>> 16));
            write(position + 2, (byte) (v >>> 8));
            write(position + 3, (byte) (v));
        }
    }

    public void writeLong(long position, long v) throws IOException {
        int bbIndex = (int) (position >> fShift);
        int bbSeek = (int) (position & fseekMask);

        if (hasRemaining(bbIndex, bbSeek, 8)) {
            bbs[bbIndex].putLong(bbSeek, v);
        } else {
            write(position, (byte) (v >>> 56));
            write(position + 1, (byte) (v >>> 48));
            write(position + 2, (byte) (v >>> 40));
            write(position + 3, (byte) (v >>> 32));
            write(position + 4, (byte) (v >>> 24));
            write(position + 5, (byte) (v >>> 16));
            write(position + 6, (byte) (v >>> 8));
            write(position + 7, (byte) (v));
        }
    }


    public long readVPLong(long fp, byte precision) throws IOException {
        int bbIndex = (int) (fp >> fShift);
        int bbSeek = (int) (fp & fseekMask);

        if (hasRemaining(bbIndex, bbSeek, precision)) {
            ByteBuffer bb = bbs[bbIndex];
            byte b = bb.get(bbSeek);
            boolean wasNegated = (b & 0x80) != 0;
            b &= ~0x80;

            long v = 0;
            v |= (b & 0xFF);
            for (int i = 1; i < precision; i++) {
                v <<= 8;
                v |= (bb.get(bbSeek + i) & 0xFF);
            }

            if (wasNegated) {
                v = -v;
            }
            return v;
        } else {
            byte b = (byte) read(fp);
            boolean wasNegated = (b & 0x80) != 0;
            b &= ~0x80;

            long v = 0;
            v |= (b & 0xFF);
            for (int i = 1; i < precision; i++) {
                v <<= 8;
                v |= (read(fp + i) & 0xFF);
            }

            if (wasNegated) {
                v = -v;
            }
            return v;
        }
    }


    public void writeVPLong(long fp, long value, byte precision) throws IOException {
        int bbIndex = (int) (fp >> fShift);
        int bbSeek = (int) (fp & fseekMask);

        boolean needsNegation = false;
        long flipped = value;
        if (flipped < 0) {
            needsNegation = true;
            flipped = -flipped;
        }

        if (hasRemaining(bbIndex, bbSeek, precision)) {
            ByteBuffer bb = bbs[bbIndex];
            for (int i = 0, j = 0; i < precision; i++, j += 8) {
                byte b = (byte) (flipped >>> j);
                int offset = precision - i - 1;
                if (offset == 0 && needsNegation) {
                    bb.put(bbSeek, (byte) (b | (0x80)));
                } else {
                    bb.put(bbSeek + offset, b);
                }
            }
        } else {
            for (int i = 0, j = 0; i < precision; i++, j += 8) {
                byte b = (byte) (flipped >>> j);
                int offset = precision - i - 1;
                if (i == offset && needsNegation) {
                    write(fp, (byte) (b | (0x80)));
                } else {
                    write(fp + precision - i - 1, b);
                }
            }
        }
    }


//    public static void main(String[] args) {
//
//        long[] values = { -Long.MAX_VALUE, -Long.MAX_VALUE / 2, -65_536, -65_535, -65_534, -128, -127, -1, 0, 1, 127, 128, 65_534, 65_535, 65_536,
//            Long.MAX_VALUE / 2, Long.MAX_VALUE };
//
//        for (long value : values) {
//            int chunkPower = UIO.chunkPower(value + 1, 1);
//            int precision = Math.min(chunkPower / 8 + 1, 8);
//
//            boolean needsNegation = false;
//            long flipped = value;
//            if (flipped < 0) {
//                needsNegation = true;
//                flipped = -flipped;
//            }
//
//            byte[] bytes = new byte[precision];
//            for (int i = 0, j = 0; i < precision; i++, j += 8) {
//                bytes[precision - i - 1] = (byte) (flipped >>> j);
//            }
//            if (needsNegation) {
//                bytes[0] |= (0x80);
//            }
//
//            System.out.println(Arrays.toString(bytes));
//
//            boolean wasNegated = (bytes[0] & 0x80) != 0;
//            bytes[0] &= ~0x80;
//
//            long output = 0;
//            for (int i = 0; i < precision; i++) {
//                output <<= 8;
//                output |= (bytes[i] & 0xFF);
//            }
//
//            if (wasNegated) {
//                output = -output;
//            }
//
//            System.out.println(value + " (" + precision + ") -> " + output + " = " + (value == output));
//        }
//
//
//    }


}
