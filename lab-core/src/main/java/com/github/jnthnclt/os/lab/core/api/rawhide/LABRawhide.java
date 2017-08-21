package com.github.jnthnclt.os.lab.core.api.rawhide;

import com.github.jnthnclt.os.lab.core.api.FormatTransformer;
import java.io.IOException;
import com.github.jnthnclt.os.lab.core.LABUtils;
import com.github.jnthnclt.os.lab.core.api.ValueStream;
import com.github.jnthnclt.os.lab.core.guts.IndexUtil;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import com.github.jnthnclt.os.lab.core.io.PointerReadableByteBufferFile;
import com.github.jnthnclt.os.lab.core.io.api.IAppendOnly;
import com.github.jnthnclt.os.lab.core.io.api.UIO;

/**
 *
 * @author jonathan.colt
 */
public class LABRawhide implements Rawhide {

    public static final String NAME = "labRawhide";
    public static final LABRawhide SINGLETON = new LABRawhide();

    private LABRawhide() {
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

        int currentKeyLength = currentRawEntry.getInt(0);
        int addingKeyLength = addingRawEntry.getInt(0);

        long currentsTimestamp = currentRawEntry.getLong(4 + currentKeyLength);
        long currentsVersion = currentRawEntry.getLong(4 + currentKeyLength + 8 + 1);

        long addingsTimestamp = addingRawEntry.getLong(4 + addingKeyLength);
        long addingsVersion = addingRawEntry.getLong(4 + addingKeyLength + 8 + 1);

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

            int aKeyLength = aRawEntry.getInt(0);
            int bKeyLength = bRawEntry.getInt(0);

            long asTimestamp = aRawEntry.getLong(4 + aKeyLength);
            long asVersion = aRawEntry.getLong(4 + aKeyLength + 8 + 1);

            long bsTimestamp = bRawEntry.getLong(4 + bKeyLength);
            long bsVersion = bRawEntry.getLong(4 + bKeyLength + 8 + 1);

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
        return rawEntrys.getLong(4 + rawEntrys.getInt(0));
    }

    @Override
    public long version(FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        BolBuffer rawEntrys) {
        return rawEntrys.getLong(4 + rawEntrys.getInt(0) + 8 + 1);
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
        int keyLength = rawEntry.getInt(0);
        BolBuffer key = rawEntry.sliceInto(4, keyLength, keyBuffer);

        long timestamp = rawEntry.getLong(4 + keyLength);
        boolean tombstone = rawEntry.get(4 + keyLength + 8) != 0;
        long version = rawEntry.getLong(4 + keyLength + 8 + 1);

        BolBuffer payload = null;
        if (valueBuffer != null) {
            int payloadLength = rawEntry.getInt(4 + keyLength + 8 + 1 + 8);
            if (payloadLength >= 0) {
                payload = rawEntry.sliceInto(4 + keyLength + 8 + 1 + 8 + 4, payloadLength, valueBuffer);
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

        rawEntryBuffer.allocate(LABUtils.rawArrayLength(key) + 8 + 1 + 8 + LABUtils.rawArrayLength(value));

        int o = 0;
        o = LABUtils.writeByteArray(key, rawEntryBuffer.bytes, o);
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
        rawEntry.sliceInto(4, rawEntry.getInt(0), keyBuffer);
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
        return "LABRawhide{" + '}';
    }

}
