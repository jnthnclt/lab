package com.github.jnthnclt.os.lab.time;

public class EpochTimestampProvider implements TimestampProvider {

    private final long epoch;

    public EpochTimestampProvider(long epoch) {
        this.epoch = epoch;
    }

    @Override
    public long getTimestamp() {
        return getApproximateTimestamp(System.currentTimeMillis());
    }

    @Override
    public long getApproximateTimestamp(long currentTimeMillis) {
        return currentTimeMillis - epoch;
    }

    @Override
    public long getApproximateTimestamp(long internalTimestamp, long wallClockDeltaMillis) {
        return internalTimestamp - wallClockDeltaMillis;
    }

}
