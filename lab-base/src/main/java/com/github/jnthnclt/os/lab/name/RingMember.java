package com.github.jnthnclt.os.lab.name;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.jnthnclt.os.lab.base.UIO;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.SignedBytes;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class RingMember implements Comparable<RingMember>, Serializable {

    public byte[] toBytes() { // TODO convert to lex byte ordering?
        byte[] bytes = new byte[1 + memberAsBytes.length];
        toBytes(bytes, 0);
        return bytes;
    }

    public int toBytes(byte[] bytes, int offset) {
        bytes[offset] = 0; // version;
        offset++;
        UIO.writeBytes(memberAsBytes, bytes, offset);
        return sizeInBytes();
    }

    public int sizeInBytes() {
        return 1 + memberAsBytes.length;
    }

    public String toBase64() {
        return BaseEncoding.base64Url().encode(toBytes());
    }

    public static RingMember fromBase64(String base64, LABInterner labInterner) throws InterruptedException {
        return labInterner.internRingMemberBase64(base64);
    }

    private final byte[] memberAsBytes;
    private final String member;
    private final int hash;

    @JsonCreator
    public RingMember(@JsonProperty("member") String member) {
        this.memberAsBytes = member.getBytes(StandardCharsets.UTF_8);
        this.member = member;

        int hashCode = 7;
        hashCode = 73 * hashCode + Arrays.hashCode(this.memberAsBytes);
        this.hash = hashCode;
    }

    public RingMember(byte[] memberAsBytes) {
        this.memberAsBytes = memberAsBytes;
        this.member = new String(memberAsBytes, StandardCharsets.UTF_8);

        int hashCode = 7;
        hashCode = 73 * hashCode + Arrays.hashCode(this.memberAsBytes);
        this.hash = hashCode;
    }

    public byte[] leakBytes() {
        return memberAsBytes;
    }

    public String getMember() {
        return member;
    }

    public AquariumMember asAquariumMember() {
        return new AquariumMember(memberAsBytes);
    }

    public static RingMember fromAquariumMember(AquariumMember member) {
        return new RingMember(member.getMember());
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        RingMember other = (RingMember) obj;
        return Arrays.equals(this.memberAsBytes, other.memberAsBytes);
    }

    @Override
    public String toString() {
        return "RingMember{" + "member=" + getMember() + '}';
    }

    @Override
    public int compareTo(RingMember o) {
        return SignedBytes.lexicographicalComparator().compare(memberAsBytes, o.memberAsBytes);
    }
}
