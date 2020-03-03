package com.github.jnthnclt.os.lab.api;

public class AppendedVersion {
    public final byte[] labId;
    public final long appendedVersion;

    public AppendedVersion(byte[] labId, long appendedVersion) {
        this.labId = labId;
        this.appendedVersion = appendedVersion;
    }
}
