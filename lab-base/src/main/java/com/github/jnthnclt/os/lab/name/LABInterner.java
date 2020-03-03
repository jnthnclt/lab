package com.github.jnthnclt.os.lab.name;

import com.github.jnthnclt.os.lab.base.UIO;
import com.github.jnthnclt.os.lab.collections.bah.ConcurrentBAHash;
import com.google.common.io.BaseEncoding;

public class LABInterner {

    private final BAInterner baInterner = new BAInterner();
    private final ConcurrentBAHash<RingMember> ringMemberInterner = new ConcurrentBAHash<>(3, true, 1024);

    public long size() {
        return ringMemberInterner.size() + baInterner.size();
    }

    public RingMember internRingMember(byte[] bytes, int offset, int length) throws InterruptedException {
        if (bytes == null || length == -1) {
            return null;
        }

        if (bytes[offset] == 0) {
            int o = offset + 1;
            int l = length - 1;
            RingMember ringMember = ringMemberInterner.get(bytes, o, l);
            if (ringMember == null) {
                byte[] key = new byte[l];
                System.arraycopy(bytes, o, key, 0, l);
                ringMember = new RingMember(key);
                ringMemberInterner.put(key, ringMember);
            }
            return ringMember;
        }
        return null;
    }

    public RingMember internRingMemberBase64(String base64) throws InterruptedException {
        byte[] bytes = BaseEncoding.base64Url().decode(base64);
        return internRingMember(bytes, 0, bytes.length);
    }

    public byte[] internRingName(byte[] key, int offset, int length) throws InterruptedException {
        return baInterner.intern(key, offset, length);
    }

    public LABName internLABName(byte[] bytes, int offset, int length) throws InterruptedException {
        if (bytes == null || length == -1) {
            return null;
        }

        int o = offset;
        if (bytes[o] == 0) { // version
            o++;
            boolean systemPartition = (bytes[o] == 1);
            o++;
            int ringNameLength = UIO.bytesInt(bytes, o);
            o += 4;
            byte[] ringName = baInterner.intern(bytes, o, ringNameLength);
            o += ringNameLength;
            int nameLength = UIO.bytesInt(bytes, o);
            o += 4;
            byte[] name = baInterner.intern(bytes, o, nameLength);
            return new LABName(systemPartition, ringName, name);
        }
        throw new RuntimeException("Invalid version:" + bytes[0]);
    }

    public LABName internLABNameBase64(String base64) throws InterruptedException {
        byte[] bytes = BaseEncoding.base64Url().decode(base64);
        return internLABName(bytes, 0, bytes.length);
    }

    public VersionedLABName internVersionedLABName(byte[] bytes, int offset, int length) throws InterruptedException {
        if (bytes == null || length == -1) {
            return null;
        }

        int o = offset;
        if (bytes[o] == 0) { // version
            o++;
            int partitionNameBytesLength = UIO.bytesInt(bytes, o);
            o += 4;
            LABName partitionName = internLABName(bytes, o, partitionNameBytesLength);
            o += partitionNameBytesLength;
            long version = UIO.bytesLong(bytes, o);
            return new VersionedLABName(partitionName, version);
        }
        throw new RuntimeException("Invalid version:" + bytes[0]);
    }

    public VersionedLABName internVersionedLABNameBase64(String base64) throws InterruptedException {
        byte[] bytes = BaseEncoding.base64Url().decode(base64);
        return internVersionedLABName(bytes, 0, bytes.length);
    }

}
