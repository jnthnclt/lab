package com.github.jnthnclt.os.lab.core.api.rawhide;

import com.github.jnthnclt.os.lab.core.guts.IndexUtil;
import com.github.jnthnclt.os.lab.core.io.PointerReadableByteBufferFile;
import java.util.Comparator;
import com.github.jnthnclt.os.lab.core.api.ValueStream;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import com.github.jnthnclt.os.lab.core.io.api.IAppendOnly;

/**
 * @author jonathan.colt
 */
public interface Rawhide {

    BolBuffer key(
        BolBuffer rawEntry,
        BolBuffer keyBuffer) throws Exception;

    boolean hasTimestampVersion();

    long timestamp(
        BolBuffer rawEntry);

    long version(
        BolBuffer rawEntry);

    boolean tombstone(
        BolBuffer rawEntry);

    boolean streamRawEntry(int index,
        BolBuffer rawEntry,
        BolBuffer keyBuffer,
        BolBuffer valueBuffer,
        ValueStream stream) throws Exception;

    BolBuffer toRawEntry(byte[] key,
        long timestamp,
        boolean tombstoned,
        long version,
        byte[] value,
        BolBuffer rawEntryBuffer) throws Exception;

    int rawEntryToBuffer(PointerReadableByteBufferFile readable, long offset, BolBuffer entryBuffer) throws Exception;

    void writeRawEntry(
        BolBuffer rawEntryBuffer,
        IAppendOnly appendOnly) throws Exception;

    // Default impls from here on out
    default BolBuffer merge(
        BolBuffer currentRawEntry,
        BolBuffer addingRawEntry) {
        return addingRawEntry;
    }

    default int mergeCompare(
        BolBuffer aRawEntry,
        BolBuffer aKeyBuffer,
        BolBuffer bRawEntry,
        BolBuffer bKeyBuffer) throws Exception {

        return compareKey( aRawEntry, aKeyBuffer,
             bRawEntry, bKeyBuffer);
    }

    default int compareKey(
        BolBuffer rawEntry,
        BolBuffer keyBuffer,
        BolBuffer compareKey
    ) throws Exception {
        return IndexUtil.compare(key(rawEntry, keyBuffer), compareKey);
    }

    default int compareKey(
        BolBuffer aRawEntry,
        BolBuffer aKeyBuffer,
        BolBuffer bRawEntry,
        BolBuffer bKeyBuffer) throws Exception {

        if (aRawEntry == null && bRawEntry == null) {
            return 0;
        } else if (aRawEntry == null) {
            return -bRawEntry.length;
        } else if (bRawEntry == null) {
            return aRawEntry.length;
        } else {
            return IndexUtil.compare(
                key( aRawEntry, aKeyBuffer),
                key( bRawEntry, bKeyBuffer)
            );
        }
    }

    Comparator<BolBuffer> bolBufferKeyComparator = IndexUtil::compare;

    default Comparator<BolBuffer> getBolBufferKeyComparator() {
        return bolBufferKeyComparator;
    }

    Comparator<byte[]> keyComparator = (byte[] o1, byte[] o2) -> IndexUtil.compare(o1, 0, o1.length, o2, 0, o2.length);

    default Comparator<byte[]> getKeyComparator() {
        return keyComparator;
    }

    default boolean mightContain(long timestamp, long timestampVersion, long newerThanTimestamp, long newerThanTimestampVersion) {
        return Rawhide.this.compare(timestamp, timestampVersion, newerThanTimestamp, newerThanTimestampVersion) >= 0;
    }

    default boolean isNewerThan(long timestamp, long timestampVersion, long newerThanTimestamp, long newerThanTimestampVersion) {
        return Rawhide.this.compare(timestamp, timestampVersion, newerThanTimestamp, newerThanTimestampVersion) > 0;
    }

    default int compare(long timestamp, long timestampVersion, long otherTimestamp, long otherTimestampVersion) {
        int c = Long.compare(timestamp, otherTimestamp);
        if (c != 0) {
            return c;
        }
        return Long.compare(timestampVersion, otherTimestampVersion);
    }

    default int compareBB(byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength) {
        return IndexUtil.compare(left, leftOffset, leftLength, right, rightOffset, rightLength);
    }

    default int compare(BolBuffer aKey, BolBuffer bKey) {
        return IndexUtil.compare(aKey, bKey);
    }

}
