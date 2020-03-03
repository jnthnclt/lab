package com.github.jnthnclt.os.lab.name;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.jnthnclt.os.lab.base.UIO;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.UnsignedBytes;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class LABName implements Comparable<LABName>, Serializable {

    private final boolean systemPartition;
    private final byte[] ringName;
    private final byte[] name;
    private transient int hash = 0;

    public byte[] toBytes() {
        byte[] asBytes = new byte[1 + 1 + 4 + ringName.length + 4 + name.length];
        asBytes[0] = 0; // version
        asBytes[1] = (byte) (systemPartition ? 1 : 0);
        UIO.intBytes(ringName.length, asBytes, 1 + 1);
        System.arraycopy(ringName, 0, asBytes, 1 + 1 + 4, ringName.length);
        UIO.intBytes(name.length, asBytes, 1 + 1 + 4 + ringName.length);
        System.arraycopy(name, 0, asBytes, 1 + 1 + 4 + ringName.length + 4, name.length);
        return asBytes;
    }

    public void toBytes(byte[] asBytes, int offset) {
        asBytes[offset + 0] = 0; // version
        asBytes[offset + 1] = (byte) (systemPartition ? 1 : 0);
        UIO.intBytes(ringName.length, asBytes, offset + 1 + 1);
        System.arraycopy(ringName, 0, asBytes, offset + 1 + 1 + 4, ringName.length);
        UIO.intBytes(name.length, asBytes, offset + 1 + 1 + 4 + ringName.length);
        System.arraycopy(name, 0, asBytes, offset + 1 + 1 + 4 + ringName.length + 4, name.length);
    }

    public int sizeInBytes() {
        return 1 + 1 + 4 + ringName.length + 4 + name.length;
    }

    @JsonCreator
    public LABName(@JsonProperty("systemPartition") boolean systemPartition,
                   @JsonProperty("ringName") byte[] ringName,
                   @JsonProperty("name") byte[] name) {
        this.systemPartition = systemPartition;
        this.ringName = ringName;
        this.name = name;
    }

    public String toBase64() {
        return BaseEncoding.base64Url().encode(toBytes());
    }

    public static LABName fromBase64(String base64, LABInterner interner) throws InterruptedException {
        return interner.internLABNameBase64(base64);
    }

    public static String toHumanReadableString(LABName labName) {
        if (Arrays.equals(labName.getRingName(), labName.getName())) {
            return new String(labName.getRingName(), StandardCharsets.UTF_8) + "::..";
        } else {
            return new String(labName.getRingName(), StandardCharsets.UTF_8) + "::" + new String(labName.getName(), StandardCharsets.UTF_8);
        }
    }

    public boolean isSystemPartition() {
        return systemPartition;
    }

    public byte[] getRingName() {
        return ringName;
    }

    public byte[] getName() {
        return name;
    }

    @Override
    public String toString() {
        return "Partition{"
            + "systemPartition=" + systemPartition
            + ", name=" + new String(name)
            + ", ring=" + new String(ringName)
            + '}';
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            int hashCode = 3;
            hashCode = 59 * hashCode + (this.systemPartition ? 1 : 0);
            hashCode = 59 * hashCode + Arrays.hashCode(this.ringName);
            hashCode = 59 * hashCode + Arrays.hashCode(this.name);
            hashCode = 59 * hashCode + 0; // legacy
            hash = hashCode;
        }
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
        final LABName other = (LABName) obj;
        if (this.systemPartition != other.systemPartition) {
            return false;
        }
        if (!Arrays.equals(this.ringName, other.ringName)) {
            return false;
        }
        return Arrays.equals(this.name, other.name);
    }

    @Override
    public int compareTo(LABName o) {
        int i = Boolean.compare(systemPartition, o.systemPartition);
        if (i != 0) {
            return i;
        }
        i = UnsignedBytes.lexicographicalComparator().compare(ringName, o.ringName);
        if (i != 0) {
            return i;
        }
        i = UnsignedBytes.lexicographicalComparator().compare(name, o.name);
        if (i != 0) {
            return i;
        }
        return i;
    }
}
