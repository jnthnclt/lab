package com.github.jnthnclt.os.lab.core;

import com.github.jnthnclt.os.lab.base.BolBuffer;
import com.github.jnthnclt.os.lab.base.UIO;
import com.github.jnthnclt.os.lab.api.ValueIndex;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class ExternalIdToInternalId {

    private final ValueIndex<byte[]> guidToIdx;

    public ExternalIdToInternalId(ValueIndex<byte[]> guidToIdx) {
        this.guidToIdx = guidToIdx;
    }

    public long[] get(long... externalIds) throws Exception {
        long[] internalIds = new long[externalIds.length];
        Arrays.fill(internalIds, -1);
        synchronized (guidToIdx) {
            guidToIdx.get(keyStream -> {
                for (int i = 0; i < externalIds.length; i++) {
                    keyStream.key(i, UIO.longBytes(externalIds[i]), 0, 8);
                }
                return true;
            }, (int index, BolBuffer key, long timestamp, boolean tombstoned, long version, BolBuffer payload) -> {
                if (payload != null) {
                    internalIds[index] = payload.getLong(0);
                }
                return true;
            }, true);
        }
        if (internalIds[0] == -1) {
            return null;
        }
        return internalIds;
    }

    public long[] getOrAllocateInternalId(Collection<Long> externalIds) throws Exception {
        return allocateIdxs(externalIds, guidToIdx);
    }

    private long[] allocateIdxs(Collection<Long> externalIds,
                                ValueIndex<byte[]> externalIdToInternalIdx) throws Exception {
        AtomicLong maxId = new AtomicLong(0);
        long[] internalIds = new long[externalIds.size()];
        Arrays.fill(internalIds, -1L);

        synchronized (externalIdToInternalIdx) {
            externalIdToInternalIdx.get(keyStream -> {
                int i = 0;
                for (Long externalId : externalIds) {
                    keyStream.key(i, UIO.longBytes(externalId), 0, 8);
                    i++;
                }
                keyStream.key(i, UIO.longBytes(Long.MAX_VALUE), 0, 8);
                return true;
            }, (int index, BolBuffer key, long timestamp, boolean tombstoned, long version, BolBuffer payload) -> {
                if (payload != null) {
                    if (index < internalIds.length) {
                        internalIds[index] = payload.getLong(0);
                    } else {
                        maxId.set(payload.getLong(0));
                    }
                }
                return true;
            }, true);

            long id = maxId.get();
            boolean[] allocatedIds = new boolean[internalIds.length];
            for (int i = 0; i < internalIds.length; i++) {
                if (internalIds[i] == -1) {
                    allocatedIds[i] = true;
                    internalIds[i] = maxId.incrementAndGet();
                }
            }

            if (maxId.get() > id) {
                long timestamp = System.currentTimeMillis(); // FIX_ME

                externalIdToInternalIdx.append(appendValueStream -> {
                    appendValueStream.stream(0, UIO.longBytes(Long.MAX_VALUE), timestamp, false, timestamp,
                            UIO.longBytes(maxId.get()));
                    int i = 0;
                    for (Long externalId : externalIds) {
                        appendValueStream.stream(i, UIO.longBytes(externalId), timestamp, false, timestamp,
                                UIO.longBytes(internalIds[i]));
                        i++;
                    }
                    return true;
                }, true, new BolBuffer(), new BolBuffer());
            }
        }
        return internalIds;
    }

    public void compact() throws Exception {
        List<Future<Object>> futures = guidToIdx.compact(true, 0, 0, true);
        for (Future<Object> future : (futures != null) ? futures : Collections.<Future<Object>>emptyList()) {
            future.get();
        }
    }

    public String summary() throws Exception {
        return "guid-to-idx: count=" + guidToIdx.count() + " debt=" + guidToIdx.debt();
    }

    public void flush() throws Exception {
        guidToIdx.commit(true, true);
    }
}
