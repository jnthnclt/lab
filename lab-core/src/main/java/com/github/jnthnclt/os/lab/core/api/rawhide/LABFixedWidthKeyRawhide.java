package com.github.jnthnclt.os.lab.core.api.rawhide;

import com.github.jnthnclt.os.lab.core.LABUtils;
import com.github.jnthnclt.os.lab.core.api.FormatTransformer;
import com.github.jnthnclt.os.lab.core.api.ValueStream;
import com.github.jnthnclt.os.lab.core.guts.IndexUtil;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import com.github.jnthnclt.os.lab.core.io.PointerReadableByteBufferFile;
import com.github.jnthnclt.os.lab.core.io.api.IAppendOnly;
import com.github.jnthnclt.os.lab.core.io.api.UIO;
import java.io.IOException;

/**
 * @author jonathan.colt
 */
public class LABFixedWidthKeyRawhide implements Rawhide {

    private final int keyLength;

    public LABFixedWidthKeyRawhide(int keyLength) {
        this.keyLength = keyLength;
    }

    @Override
    public BolBuffer merge(FormatTransformer currentReadKeyFormatTransformer,
        FormatTransformer currentReadValueFormatTransformer,
        BolBuffer currentRawEntry,
        FormatTransformer addingReadKeyFormatTransformer,
        FormatTransformer addingReadValueFormatTransformer,
        BolBuffer addingRawEntry,
        FormatTransformer mergedReadKeyFormatTransformer,
        FormatTransformer mergedReadValueFormatTransformer) {

        int currentKeyLength = keyLength;
        int addingKeyLength = keyLength;

        long currentsTimestamp = currentRawEntry.getLong(currentKeyLength);
        long currentsVersion = currentRawEntry.getLong(currentKeyLength + 8 + 1);

        long addingsTimestamp = addingRawEntry.getLong(addingKeyLength);
        long addingsVersion = addingRawEntry.getLong(addingKeyLength + 8 + 1);

        if ((currentsTimestamp > addingsTimestamp) || (currentsTimestamp == addingsTimestamp && currentsVersion > addingsVersion)) {
            return currentRawEntry;
        } else {
            return addingRawEntry;
        }
    }

    @Override
    public int mergeCompare(FormatTransformer aReadKeyFormatTransformer,
        FormatTransformer aReadValueFormatTransformer,
        BolBuffer aRawEntry,
        BolBuffer aKeyBuffer,
        FormatTransformer bReadKeyFormatTransformer,
        FormatTransformer bReadValueFormatTransformer,
        BolBuffer bRawEntry,
        BolBuffer bKeyBuffer) throws Exception {

        int c = compareKey(aReadKeyFormatTransformer,
            aReadValueFormatTransformer,
            aRawEntry,
            aKeyBuffer,
            bReadKeyFormatTransformer,
            bReadValueFormatTransformer,
            bRawEntry,
            bKeyBuffer);
        if (c != 0) {
            return c;
        }

        if (aRawEntry == null && bRawEntry == null) {
            return 0;
        } else if (aRawEntry == null) {
            return -bRawEntry.length;
        } else if (bRawEntry == null) {
            return aRawEntry.length;
        } else {

            int aKeyLength = keyLength;
            int bKeyLength = keyLength;

            long asTimestamp = aRawEntry.getLong(aKeyLength);
            long asVersion = aRawEntry.getLong(aKeyLength + 8 + 1);

            long bsTimestamp = bRawEntry.getLong(bKeyLength);
            long bsVersion = bRawEntry.getLong(bKeyLength + 8 + 1);

            if (asTimestamp == bsTimestamp && asVersion == bsVersion) {
                return 0;
            }
            if ((asTimestamp > bsTimestamp) || (asTimestamp == bsTimestamp && asVersion > bsVersion)) {
                return -1;
            } else {
                return 1;
            }
        }
    }

    @Override
    public boolean hasTimestampVersion() {
        return true;
    }

    @Override
    public long timestamp(FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        BolBuffer rawEntrys) {
        return rawEntrys.getLong(keyLength);
    }

    @Override
    public long version(FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        BolBuffer rawEntrys) {
        return rawEntrys.getLong(keyLength + 8 + 1);
    }

    @Override
    public boolean tombstone(FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        BolBuffer rawEntrys) {
        return rawEntrys.get(keyLength + 8) != 0;
    }

    @Override
    public boolean streamRawEntry(int index,
        FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        BolBuffer rawEntry,
        BolBuffer keyBuffer,
        BolBuffer valueBuffer,
        ValueStream stream) throws Exception {

        if (rawEntry == null) {
            return stream.stream(index, null, -1, false, -1, null);
        }
        BolBuffer key = rawEntry.sliceInto(0, keyLength, keyBuffer);

        long timestamp = rawEntry.getLong( keyLength);
        boolean tombstone = rawEntry.get( keyLength + 8) != 0;
        long version = rawEntry.getLong(keyLength + 8 + 1);

        BolBuffer payload = null;
        if (valueBuffer != null) {
            int payloadLength = rawEntry.getInt(keyLength + 8 + 1 + 8);
            if (payloadLength >= 0) {
                payload = rawEntry.sliceInto( keyLength + 8 + 1 + 8 + 4, payloadLength, valueBuffer);
            }
        }
        return stream.stream(index, readKeyFormatTransformer.transform(key), timestamp, tombstone, version, readValueFormatTransformer.transform(payload));
    }

    @Override
    public BolBuffer toRawEntry(
        byte[] key,
        long timestamp,
        boolean tombstoned,
        long version,
        byte[] value,
        BolBuffer rawEntryBuffer) throws IOException {

        rawEntryBuffer.allocate(keyLength + 8 + 1 + 8 + LABUtils.rawArrayLength(value));

        int o = 0;
        o = LABUtils.writeFixedWidthByteArray(key, rawEntryBuffer.bytes, o);
        UIO.longBytes(timestamp, rawEntryBuffer.bytes, o);
        o += 8;
        rawEntryBuffer.bytes[o] = tombstoned ? (byte) 1 : (byte) 0;
        o++;
        UIO.longBytes(version, rawEntryBuffer.bytes, o);
        o += 8;
        LABUtils.writeByteArray(value, rawEntryBuffer.bytes, o);
        return rawEntryBuffer;
    }

    @Override
    public int rawEntryToBuffer(PointerReadableByteBufferFile readable, long offset, BolBuffer entryBuffer) throws Exception {
        int length = readable.readInt(offset);
        readable.sliceIntoBuffer(offset + 4, length - 8, entryBuffer);
        return length;
    }

    @Override
    public void writeRawEntry(FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        BolBuffer rawEntryBuffer,
        FormatTransformer writeKeyFormatTransformer,
        FormatTransformer writeValueFormatTransformer,
        IAppendOnly appendOnly) throws Exception {

        int entryLength = 4 + rawEntryBuffer.length + 4;
        appendOnly.appendInt(entryLength);
        appendOnly.append(rawEntryBuffer);
        appendOnly.appendInt(entryLength);
    }

    @Override
    public BolBuffer key(FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        BolBuffer rawEntry,
        BolBuffer keyBuffer) {
        rawEntry.sliceInto(0, keyLength, keyBuffer);
        return keyBuffer;
    }

    @Override
    public int compareKey(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransformer,
        BolBuffer rawEntry,
        BolBuffer keyBuffer,
        BolBuffer compareKey
    ) {
        return IndexUtil.compare(key(readKeyFormatTransormer, readValueFormatTransformer, rawEntry, keyBuffer), compareKey);
    }

    @Override
    public String toString() {
        return "LABFixedWidthKeyRawhide{" + '}';
    }

}
