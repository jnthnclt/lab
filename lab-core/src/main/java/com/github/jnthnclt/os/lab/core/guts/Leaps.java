package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.base.BolBuffer;
import com.github.jnthnclt.os.lab.core.io.PointerReadableByteBufferFile;
import com.github.jnthnclt.os.lab.io.IAppendOnly;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.LongBuffer;
import java.util.Arrays;

/**
 *
 * @author jonathan.colt
 */
public class Leaps {

    private final int index;
    final BolBuffer lastKey;
    final long[] fps;
    final BolBuffer[] keys;
    final StartOfEntry startOfEntry;

    public interface StartOfEntry {

        LongBuffer get(PointerReadableByteBufferFile readable) throws IOException;
    }

    public Leaps(int index, BolBuffer lastKey, long[] fpIndex, BolBuffer[] keys, StartOfEntry startOfEntry) {
        this.index = index;
        this.lastKey = lastKey;
        Preconditions.checkArgument(fpIndex.length == keys.length, "fpIndex and keys misalignment, %s != %s", fpIndex.length, keys.length);
        this.fps = fpIndex;
        this.keys = keys;
        this.startOfEntry = startOfEntry;
    }

    public String toString(PointerReadableByteBufferFile pointerReadable) throws IOException {
        StringBuilder startOfEntryString = new StringBuilder();
        LongBuffer longBuffer = startOfEntry.get(pointerReadable);
        for (int i = 0; i < fps.length; i++) {
            if (i > 0) {
                startOfEntryString.append(", ");
            }
            startOfEntryString.append(longBuffer.get(i));
        }

        StringBuilder keyString = new StringBuilder();
        for (int i = 0; i < keys.length; i++) {
            if (i > 0) {
                keyString.append(", ");
            }
            byte[] copy = keys[i].copy();
            keyString.append(Arrays.toString(copy));
        }

        return "Leaps{"
            + "index=" + index
            + ", lastKey=" + Arrays.toString(lastKey.copy())
            + ", fps=" + Arrays.toString(fps)
            + ", keys=" + keyString.toString()
            + ", startOfEntry=" + startOfEntryString.toString()
            + '}';
    }

    public void write(IAppendOnly writeable) throws Exception {
        BolBuffer writeLastKey = lastKey;
        BolBuffer[] writeKeys = keys;

        LongBuffer startOfEntryBuffer = startOfEntry.get(null);
        int entryLength = 4 + 4 + 4 + writeLastKey.length + 4 + (startOfEntryBuffer.limit() * 8) + 4;
        for (int i = 0; i < fps.length; i++) {
            entryLength += 8 + 4 + writeKeys[i].length;
        }
        entryLength += 4;

        writeable.appendInt(entryLength);
        writeable.appendInt(index);
        writeable.appendInt(writeLastKey.length);

        writeable.append(writeLastKey);

        writeable.appendInt(fps.length);
        for (int i = 0; i < fps.length; i++) {
            writeable.appendLong(fps[i]);
            writeable.appendInt(writeKeys[i].length);
            writeable.append(writeKeys[i]);
        }
        int startOfEntryLength = startOfEntryBuffer.limit();
         writeable.appendInt(startOfEntryLength);
        for (int i = 0; i < startOfEntryLength; i++) {
            writeable.appendLong(startOfEntryBuffer.get(i));
        }
        writeable.appendInt(entryLength);
    }

    static Leaps read(PointerReadableByteBufferFile readable, long offset) throws Exception {
        int entryLength = readable.readInt(offset);
        offset += 4;
        int index = readable.readInt(offset);
        offset += 4;
        int lastKeyLength = readable.readInt(offset);
        offset += 4;

        byte[] lastKey = new byte[lastKeyLength];
        readable.read(offset, lastKey, 0, lastKeyLength);
        offset += lastKeyLength;

        int fpIndexLength = readable.readInt(offset);
        offset += 4;

        long[] fpIndex = new long[fpIndexLength];
        BolBuffer[] keys = new BolBuffer[fpIndexLength];
        for (int i = 0; i < fpIndexLength; i++) {

            fpIndex[i] = readable.readLong(offset);
            offset += 8;
            int keyLength = readable.readInt(offset);
            offset += 4;
            keys[i] = readable.sliceIntoBuffer(offset, keyLength, new BolBuffer());
            offset += keyLength;
        }
        int startOfEntryLength = readable.readInt(offset);
        offset += 4;
        long startOfEntryOffset = offset;
        int startOfEntryNumBytes = startOfEntryLength * 8;
        StartOfEntry startOfEntry = (readable1) -> {
            BolBuffer sliceIntoBuffer = readable1.sliceIntoBuffer(startOfEntryOffset, startOfEntryNumBytes, new BolBuffer());
            return sliceIntoBuffer.asLongBuffer();
        };

        offset += startOfEntryNumBytes;
        if (readable.readInt(offset) != entryLength) {
            throw new RuntimeException("Encountered length corruption. ");
        }
        return new Leaps(index, new BolBuffer(lastKey), fpIndex,keys, startOfEntry);
    }

}
