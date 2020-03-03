package com.github.jnthnclt.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public class CompareTimestampVersions {

    public static final int compare(long aTimestamp, long aVersion, long bTimestamp, long bVersion) {
        if (aTimestamp == bTimestamp) {
            return Long.compare(aVersion, bVersion);
        }
        return Long.compare(aTimestamp, bTimestamp);
    }
}
