package com.github.jnthnclt.os.lab.name;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.jnthnclt.os.lab.base.UIO;
import com.google.common.io.BaseEncoding;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;


/**
 * @author jonathan.colt
 */
public class VersionedLABName implements Comparable<VersionedLABName>, Serializable {

    public static final long STATIC_VERSION = 0;

    private final LABName labName;
    private final long labVersion;
    private transient int hash = 0;

    public byte[] toBytes() throws IOException {
        byte[] labNameBytes = labName.toBytes();

        byte[] asBytes = new byte[1 + 4 + labNameBytes.length + 8];
        asBytes[0] = 0; // version
        UIO.intBytes(labNameBytes.length, asBytes, 1);
        System.arraycopy(labNameBytes, 0, asBytes, 1 + 4, labNameBytes.length);
        UIO.longBytes(labVersion, asBytes, 1 + 4 + labNameBytes.length);
        return asBytes;
    }

    public int sizeInBytes() {
        return 1 + 4 + labName.sizeInBytes() + 8;
    }

    @JsonCreator
    public VersionedLABName(@JsonProperty("labName") LABName labName,
                            @JsonProperty("labVersion") long labVersion) {
        this.labName = labName;
        this.labVersion = labVersion;
    }

    public String toBase64() throws IOException {
        return BaseEncoding.base64Url().encode(toBytes());
    }

    public static VersionedLABName fromBase64(String base64, LABInterner interner) throws Exception {
        return interner.internVersionedLABNameBase64(base64);
    }

    public LABName getLabName() {
        return labName;
    }

    public long getPartitionVersion() {
        return labVersion;
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            int h = 3;
            h = 31 * h + Objects.hashCode(this.labName);
            h = 31 * h + (int) (this.labVersion ^ (this.labVersion >>> 32));
            this.hash = h;
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
        final VersionedLABName other = (VersionedLABName) obj;
        if (!Objects.equals(this.labName, other.labName)) {
            return false;
        }
        return this.labVersion == other.labVersion;
    }

    @Override
    public int compareTo(VersionedLABName o) {
        int i = labName.compareTo(o.labName);
        if (i != 0) {
            return i;
        }
        return Long.compare(labVersion, o.labVersion);
    }

    @Override
    public String toString() {
        return "VersionedLABName{" + "labName=" + labName + ", labVersion=" + labVersion + '}';
    }

}
