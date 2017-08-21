package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.core.io.PointerReadableByteBufferFile;
import java.io.IOException;
import java.util.Arrays;
import com.github.jnthnclt.os.lab.core.io.api.IAppendOnly;
import com.github.jnthnclt.os.lab.core.io.api.UIO;

/**
 *
 * @author jonathan.colt
 */
public class Footer {

    final int leapCount;
    final long count;
    final long keysSizeInBytes;
    final long valuesSizeInBytes;
    final byte[] minKey;
    final byte[] maxKey;
    final long keyFormat;
    final long valueFormat;
    final long maxTimestamp;
    final long maxTimestampVersion;

    public Footer(int leapCount,
        long count,
        long keysSizeInBytes,
        long valuesSizeInBytes,
        byte[] minKey,
        byte[] maxKey,
        long keyFormat,
        long valueFormat,
        long maxTimestamp,
        long maxTimestampVersion
    ) {

        this.leapCount = leapCount;
        this.count = count;
        this.keysSizeInBytes = keysSizeInBytes;
        this.valuesSizeInBytes = valuesSizeInBytes;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.keyFormat = keyFormat;
        this.valueFormat = valueFormat;
        this.maxTimestamp = maxTimestamp;
        this.maxTimestampVersion = maxTimestampVersion;
    }

    @Override
    public String toString() {
        return "Footer{"
            + "leapCount=" + leapCount
            + ", count=" + count
            + ", keysSizeInBytes=" + keysSizeInBytes
            + ", valuesSizeInBytes=" + valuesSizeInBytes
            + ", minKey=" + Arrays.toString(minKey)
            + ", maxKey=" + Arrays.toString(maxKey)
            + ", keyFormat=" + keyFormat
            + ", valueFormat=" + valueFormat
            + ", maxTimestamp=" + maxTimestamp
            + ", maxTimestampVersion=" + maxTimestampVersion
            + '}';
    }

    public void write(IAppendOnly writeable) throws IOException {
        int entryLength = 4 + 4 + 8 + 8 + 8 + 4 + (minKey == null ? 0 : minKey.length) + 4 + (maxKey == null ? 0 : maxKey.length) + 8 + 8 + 8 + 8 + 4;
        writeable.appendInt(entryLength);
        writeable.appendInt(leapCount);
        writeable.appendLong(count);
        writeable.appendLong(keysSizeInBytes);
        writeable.appendLong(valuesSizeInBytes);
        UIO.writeByteArray(writeable, minKey, "minKey");
        UIO.writeByteArray(writeable, maxKey, "maxKey");
        writeable.appendLong(maxTimestamp);
        writeable.appendLong(maxTimestampVersion);
        writeable.appendLong(keyFormat);
        writeable.appendLong(valueFormat);
        writeable.appendInt(entryLength);
    }

    static Footer read(PointerReadableByteBufferFile readable, long offset) throws IOException {
        long initialOffset = offset;
        int entryLength = readable.readInt(offset);
        offset += 4;
        int leapCount = readable.readInt(offset);
        offset += 4;
        long count = readable.readLong(offset);
        offset += 8;
        long keysSizeInBytes = readable.readLong(offset);
        offset += 8;
        long valuesSizeInBytes = readable.readLong(offset);
        offset += 8;

        int minKeyLength = readable.readInt(offset);
        offset += 4;
        byte[] minKey = null;
        if (minKeyLength > -1) {
            minKey = new byte[minKeyLength];
            readable.read(offset, minKey, 0, minKeyLength);
            offset += minKeyLength;
        }

        int maxKeyLength = readable.readInt(offset);
        offset += 4;
        byte[] maxKey = null;
        if (maxKeyLength > -1) {
            maxKey = new byte[maxKeyLength];
            readable.read(offset, maxKey, 0, maxKeyLength);
            offset += maxKeyLength;
        }
        long maxTimestamp = readable.readLong(offset);
        offset += 8;
        long maxTimestampVersion = readable.readLong(offset);
        offset += 8;

        long keyFormat = 0;
        long valueFormat = 0;
        if (entryLength == (offset - initialOffset) + 8 + 8 + 4) {
            keyFormat = readable.readLong(offset);
            offset += 8;
            valueFormat = readable.readLong(offset);
            offset += 8;
        }

        long el = readable.readInt(offset);
        if (el != entryLength) {
            throw new RuntimeException("Encountered length corruption. " + el + " vs " + entryLength);
        }
        return new Footer(leapCount, count, keysSizeInBytes, valuesSizeInBytes, minKey, maxKey, keyFormat, valueFormat, maxTimestamp, maxTimestampVersion);
    }

}
