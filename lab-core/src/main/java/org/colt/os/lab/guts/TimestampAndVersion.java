package org.colt.os.lab.guts;

/**
 *
 * @author jonathan.colt
 */
public class TimestampAndVersion {

    public static final TimestampAndVersion NULL = new TimestampAndVersion(-1, -1);

    public final long maxTimestamp;
    public final long maxTimestampVersion;

    public TimestampAndVersion(long maxTimestamp, long maxTimestampVersion) {
        this.maxTimestamp = maxTimestamp;
        this.maxTimestampVersion = maxTimestampVersion;
    }

    @Override
    public String toString() {
        return "TimestampAndVersion{" + "maxTimestamp=" + maxTimestamp + ", maxTimestampVersion=" + maxTimestampVersion + '}';
    }

}
