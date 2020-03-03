package com.github.jnthnclt.os.lab.order;

public interface LABTimestampProvider {

    long getTimestamp();

    long getApproximateTimestamp(long wallClockCurrentTimeMillis);

    /**
     * @param timestampId          This is an id that is internal to the TimestampProvider. Put another way, this is the value getTimestamp() returns;
     * @param wallClockDeltaMillis
     * @return
     */
    long getApproximateTimestamp(long timestampId, long wallClockDeltaMillis);
}
