package com.github.jnthnclt.os.lab.core.sort;

import com.github.jnthnclt.os.lab.base.BolBuffer;
import com.github.jnthnclt.os.lab.base.IndexUtil;
import com.github.jnthnclt.os.lab.base.UIO;
import com.github.jnthnclt.os.lab.core.ExternalIdToInternalId;
import com.github.jnthnclt.os.lab.api.ValueIndex;
import com.github.jnthnclt.os.lab.time.TimestampProvider;

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class BitSortIndex {

    private final TimestampProvider timestampProvider;
    private final ExternalIdToInternalId externalIdToInternalId;
    private final ValueIndex<byte[]> externalIdToSortValue;
    private final ValueIndex<byte[]> sortValueAndId;
    private final AtomicReference<BitSortTree> bitSortTree = new AtomicReference<>();
    private final Comparator<BolBuffer> lexKeyComparator = IndexUtil::compare;
    private final AtomicLong version = new AtomicLong();

    public BitSortIndex(TimestampProvider timestampProvider,
                        ExternalIdToInternalId externalIdToInternalId,
                        ValueIndex<byte[]> externalIdToSortValue,
                        ValueIndex<byte[]> sortValueAndId) {

        this.timestampProvider = timestampProvider;
        this.externalIdToInternalId = externalIdToInternalId;
        this.externalIdToSortValue = externalIdToSortValue;
        this.sortValueAndId = sortValueAndId;
    }

    public interface BitSortIndexUpdates {
        boolean updates(BitSortIndexUpdate update) throws Exception;
    }

    public interface BitSortIndexUpdate {
        boolean update(long id, byte[] sortValue, boolean tombstone) throws Exception;
    }

    public void update(BitSortIndexUpdates updates) throws Exception {
        long timestamp = timestampProvider.getTimestamp();
        sortValueAndId.append(sortValueStream -> {

            externalIdToSortValue.append(idValueStream -> {

                return updates.updates((id, sortValue, tombstone) -> {

                    if (!sortValueStream.stream(-1,
                            UIO.longBytes(id),
                            timestamp,
                            tombstone,
                            version.incrementAndGet(),
                            sortValue)) {
                        return false;
                    };

                    if (!idValueStream.stream(-1,
                            UIO.longBytes(id),
                            timestamp,
                            tombstone,
                            version.incrementAndGet(),
                            sortValue)) {
                        return false;
                    }
                    return true;
                });

            }, true, new BolBuffer(), new BolBuffer());

            return true;
        }, true, new BolBuffer(), new BolBuffer());

    }

    public void rebuildIndex() {

//        BitSortTree bitSortTree = new BitSortTree();
//        bitSortTree.populate(input, 512);
    }
}
