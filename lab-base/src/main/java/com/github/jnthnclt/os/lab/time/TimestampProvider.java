package com.github.jnthnclt.os.lab.time;

public interface TimestampProvider {

    long getTimestamp();

    long getApproximateTimestamp(long wallClockCurrentTimeMillis);

    /**
     * @param timestampId          This is an id that is internal to the TimestampProvider. Put another way, this is the ordinal getTimestamp() returns;
     * @param wallClockDeltaMillis
     * @return timestamp
     */
    long getApproximateTimestamp(long timestampId, long wallClockDeltaMillis);
}
