package com.github.jnthnclt.os.lab.time;

import java.util.concurrent.atomic.AtomicReference;

public class SequenceIdProviderImpl {
    /**
     * An sequence id provider which generates ids using a composite key of
     * timestamp, uniqueId, and an sequence number.
     * 
     * @see <a href="https://medium.com/@varuntayal/what-does-it-take-to-generate-cluster-wide-unique-ids-in-a-distributed-system-d505b9eaa46e">Article</a>
     * 
     * 
     */
    private final UniqueId uniqueId;
    private final SnowflakeIdPacker idPacker;
    private final EpochTimestampProvider timestampProvider;

    private final int maxSequenceId;
    private final AtomicReference<TimeAndOrder> state;

    SequenceIdProviderImpl(UniqueId writerId, long epoch) {
        this(writerId, new SnowflakeIdPacker(), new EpochTimestampProvider(epoch));
    }

    SequenceIdProviderImpl(UniqueId uniqueId, SnowflakeIdPacker idPacker, EpochTimestampProvider timestampProvider) {
        this.uniqueId = uniqueId;
        this.idPacker = idPacker;
        this.timestampProvider = timestampProvider;
        this.maxSequenceId = SnowflakeIdPacker.getMaxSequenceId();
        this.state = new AtomicReference<>(new TimeAndOrder(timestampProvider.getTimestamp(), 0));
    }

    long nextId() {
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

                    if (nextOrder > maxSequenceId) {
                        continue;
                    }
                    next = new TimeAndOrder(timestamp, nextOrder);
                } else {
                    next = new TimeAndOrder(timestamp, 0);
                }

                if (state.compareAndSet(current, next)) {
                    UniqueId writerId;
                    long id;
                    do {
                        writerId = getUniqueId();
                        id = idPacker.pack(next.time, writerId.getId(), next.order);
                    } while (!writerId.isValid());
                    return id;
                }
            }
        }
    }

    private UniqueId getUniqueId() {
        int maxUniqueId = SnowflakeIdPacker.getMaxUniqueId();
        if (uniqueId.getId() < 0 || uniqueId.getId() > maxUniqueId) {
            throw new IllegalArgumentException("uniqueId is out of range must be 0.." + maxUniqueId);
        }
        return uniqueId;
    }

    public long[] unpack(long id) {
        return idPacker.unpack(id);
    }

    /* if you want to be annoying, you could avoid this object by packing the state
       in a long and test/set an AtomicLong */
    private static final class TimeAndOrder {

        TimeAndOrder(long time, int order) {
            this.time = time;
            this.order = order;
        }

        final long time;
        final int order;
    }
}
