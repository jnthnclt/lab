package com.github.jnthnclt.os.lab.time;

/**
 * Uses 42bits for time, 9bits for writerId, 12bits for orderId and 1 bit for add vs removed.
 */
class SnowflakeIdPacker {

    public static final int TIMESTAMP_PRECISION = 42;
    public static final int UNIQUE_ID_PRECISION = 9;
    public static final int SEQUENCE_ID_PRECISION = 12;

    public int bitsPrecisionOfTimestamp() {
        return TIMESTAMP_PRECISION;
    }

    long pack(long timestamp, int writerId, int orderId) {
        long id = (timestamp & 0x1FFFFFFFFFFL) << 9 + 12 + 1;
        id |= ((writerId & 0x1FF) << 12 + 1);
        id |= ((orderId & 0xFFF) << 1);
        return id;
    }

    public long[] unpack(long packedId) {
        long time = (packedId & (0x1FFFFFFFFFFL << 9 + 12 + 1)) >>> 9 + 12 + 1;
        int writer = (int) ((packedId & (0x1FF << 12 + 1)) >>> 12 + 1);
        int order = (int) ((packedId & (0xFFF << 1)) >>> 1);
        return new long[] { time, writer, order };
    }

    static int getMaxUniqueId() {
        return getMaxValue(UNIQUE_ID_PRECISION);
    }

    static int getMaxSequenceId() {
        return getMaxValue(SEQUENCE_ID_PRECISION);
    }

    static private int getMaxValue(int bits) {
        return (int) Math.pow(2, bits) - 1;
    }
}

