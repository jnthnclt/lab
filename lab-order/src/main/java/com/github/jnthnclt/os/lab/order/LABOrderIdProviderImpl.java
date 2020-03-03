package com.github.jnthnclt.os.lab.order;

import java.util.concurrent.atomic.AtomicReference;

/**
 * An order id provider which generates ids using a combination of system time, a logical writer id, and an incrementing sequence number.
 */
public class LABOrderIdProviderImpl implements LABOrderIdProvider {

    private final LABWriterId writerId;
    private final SnowflakeIdPacker idPacker;
    private final LABEpochLABTimestampProvider timestampProvider;

    private final int maxOrderId;
    private final AtomicReference<TimeAndOrder> state;

    public LABOrderIdProviderImpl(LABWriterId writerId) {
        this(writerId, new SnowflakeIdPacker(), new LABEpochLABTimestampProvider());
    }

    public LABOrderIdProviderImpl(LABWriterId writerId, SnowflakeIdPacker idPacker, LABEpochLABTimestampProvider timestampProvider) {
        this.writerId = writerId;
        this.idPacker = idPacker;
        this.timestampProvider = timestampProvider;
        this.maxOrderId = SnowflakeIdPacker.getMaxOrderId();
        this.state = new AtomicReference<>(new TimeAndOrder(timestampProvider.getTimestamp(), 0));
    }

    @Override
    public long nextId() {
        while (true) { // exits on successful compare-and-set
            long timestamp = timestampProvider.getTimestamp();
            TimeAndOrder current = state.get();

            if (current.time > timestamp) {
                long retryWaitHint = current.time - timestamp;

                try {
                    Thread.sleep(retryWaitHint);
                } catch (InterruptedException ie) {
                    Thread.interrupted();
                }

            } else {
                TimeAndOrder next;

                if (current.time == timestamp) {
                    int nextOrder = current.order + 1;

                    if (nextOrder > maxOrderId) {
                        continue;
                    }

                    next = new TimeAndOrder(timestamp, nextOrder);
                } else {
                    next = new TimeAndOrder(timestamp, 0);
                }

                if (state.compareAndSet(current, next)) {
                    LABWriterId writerId;
                    long id;
                    do {
                        writerId = getWriterId();
                        id = idPacker.pack(next.time, writerId.getId(), next.order);
                    }
                    while (!writerId.isValid());
                    return id;
                }
            }
        }
    }

    private LABWriterId getWriterId() {
        int maxWriterId = SnowflakeIdPacker.getMaxWriterId();
        if (writerId.getId() < 0 || writerId.getId() > maxWriterId) {
            throw new IllegalArgumentException("writerId is out of range must be 0.." + maxWriterId);
        }
        return writerId;
    }

    public long[] unpack(long id) {
        return idPacker.unpack(id);
    }

    //if you want to be annoying, you could avoid this object by packing the state in a long and test/set an AtomicLong
    private static final class TimeAndOrder {

        TimeAndOrder(long time, int order) {
            this.time = time;
            this.order = order;
        }

        final long time;
        final int order;
    }
}
