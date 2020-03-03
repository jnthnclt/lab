package com.github.jnthnclt.os.lab.order;

class LABEpochLABTimestampProvider implements LABTimestampProvider {

    private static final long LAB_EPOCH = 1500316088258L; // Mon July 17, 2017 EOA epoch

    @Override
    public long getTimestamp() {
        return getApproximateTimestamp(System.currentTimeMillis());
    }

    @Override
    public long getApproximateTimestamp(long currentTimeMillis) {
        return currentTimeMillis - LAB_EPOCH;
    }

    @Override
    public long getApproximateTimestamp(long internalTimestamp, long wallClockDeltaMillis) {
        return internalTimestamp - wallClockDeltaMillis;
    }
}
