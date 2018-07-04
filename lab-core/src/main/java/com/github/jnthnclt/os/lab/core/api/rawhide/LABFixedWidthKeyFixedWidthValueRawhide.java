package com.github.jnthnclt.os.lab.core.api.rawhide;

import com.github.jnthnclt.os.lab.base.BolBuffer;
import com.github.jnthnclt.os.lab.core.api.ValueStream;
import com.github.jnthnclt.os.lab.core.io.PointerReadableByteBufferFile;
import com.github.jnthnclt.os.lab.io.IAppendOnly;

/**
 *
 * @author jonathan.colt
 */
public class LABFixedWidthKeyFixedWidthValueRawhide implements Rawhide {

    private final int keyLength;
    private final int payloadLength;

    public LABFixedWidthKeyFixedWidthValueRawhide(int keyLength, int payloadLength) {
        this.keyLength = keyLength;
        this.payloadLength = payloadLength;
    }

    @Override
    public boolean streamRawEntry(int index,
        BolBuffer rawEntry,
        BolBuffer keyBuffer,
        BolBuffer valueBuffer,
        ValueStream stream) throws Exception {

        if (rawEntry == null) {
            return stream.stream(index, null, -1, false, -1, null);
        }

        BolBuffer key = rawEntry.sliceInto(0, keyLength, keyBuffer);
        BolBuffer payload = null;
        if (valueBuffer != null) {
            payload = rawEntry.sliceInto(keyLength, payloadLength, valueBuffer);
        }
        return stream.stream(index, key, 0, false, 0, payload);
    }

    @Override
    public BolBuffer toRawEntry(byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload, BolBuffer rawEntryBuffer) throws
        Exception {

        rawEntryBuffer.allocate(keyLength + payloadLength);
        System.arraycopy(key, 0, rawEntryBuffer.bytes, 0, keyLength);
        if (payloadLength > 0) {
            System.arraycopy(payload, 0, rawEntryBuffer.bytes, keyLength, payloadLength);
        }
        return rawEntryBuffer;
    }

    @Override
    public int rawEntryToBuffer(PointerReadableByteBufferFile readable, long offset, BolBuffer entryBuffer) throws Exception {
        readable.sliceIntoBuffer(offset, keyLength + payloadLength, entryBuffer);
        return keyLength + payloadLength;
    }

    @Override
    public void writeRawEntry(
        BolBuffer rawEntryBuffer,
        IAppendOnly appendOnly) throws Exception {
        appendOnly.append(rawEntryBuffer);
    }

    @Override
    public BolBuffer key(
        BolBuffer rawEntry,
        BolBuffer keyBuffer) {
        rawEntry.sliceInto(0, keyLength, keyBuffer);
        return keyBuffer;
    }

    @Override
    public boolean hasTimestampVersion() {
        return false;
    }

    @Override
    public long timestamp( BolBuffer rawEntry) {
        return 0;
    }

    @Override
    public long version( BolBuffer rawEntry) {
        return 0;
    }

    @Override
    public boolean tombstone(BolBuffer rawEntry) {
        return false;
    }

    @Override
    public boolean isNewerThan(long timestamp, long timestampVersion, long newerThanTimestamp, long newerThanTimestampVersion) {
        return true;
    }

    @Override
    public boolean mightContain(long timestamp, long timestampVersion, long newerThanTimestamp, long newerThanTimestampVersion) {
        return (timestamp != -1 && timestampVersion != -1);
    }

}
