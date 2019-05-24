package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.base.BolBuffer;
import com.github.jnthnclt.os.lab.base.UIO;
import com.github.jnthnclt.os.lab.core.LABStats;
import com.github.jnthnclt.os.lab.core.api.exceptions.LABClosedException;
import com.github.jnthnclt.os.lab.core.api.exceptions.LABConcurrentSplitException;
import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.guts.api.CommitIndex;
import com.github.jnthnclt.os.lab.core.guts.api.IndexFactory;
import com.github.jnthnclt.os.lab.core.guts.api.KeyToString;
import com.github.jnthnclt.os.lab.core.guts.api.MergerBuilder;
import com.github.jnthnclt.os.lab.core.guts.api.ReadIndex;
import com.github.jnthnclt.os.lab.core.guts.api.SplitterBuilder;
import com.github.jnthnclt.os.lab.log.LABLogger;
import com.github.jnthnclt.os.lab.log.LABLoggerFactory;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author jonathan.colt
 */
public class CompactableIndexes {

    static private class IndexesLock {
    }

    private static final LABLogger LOG = LABLoggerFactory.getLogger();

    // newest to oldest
    private final LABStats stats;
    private final Rawhide rawhide;
    private final IndexesLock indexesLock = new IndexesLock();
    private volatile boolean[] merging = new boolean[0]; // is volatile for reference changes not value changes.
    private volatile ReadOnlyIndex[] indexes = new ReadOnlyIndex[0];  // is volatile for reference changes not value changes.
    private volatile long version;
    private volatile boolean disposed = false;
    private volatile boolean closed = false;
    private final AtomicBoolean compacting = new AtomicBoolean();
    private volatile TimestampAndVersion maxTimestampAndVersion = TimestampAndVersion.NULL;

    public CompactableIndexes(LABStats stats, Rawhide rawhide) {
        this.stats = stats;
        this.rawhide = rawhide;
    }

    public boolean append(ReadOnlyIndex index) {
        int indexLengthChange;
        synchronized (indexesLock) {
            if (disposed) {
                return false;
            }

            int length = indexes.length + 1;
            boolean[] prependToMerging = new boolean[length];
            prependToMerging[0] = false;
            System.arraycopy(merging, 0, prependToMerging, 1, merging.length);

            ReadOnlyIndex[] prependToIndexes = new ReadOnlyIndex[length];
            prependToIndexes[0] = index;
            System.arraycopy(indexes, 0, prependToIndexes, 1, indexes.length);

            merging = prependToMerging;
            indexLengthChange = prependToIndexes.length - indexes.length;
            indexes = prependToIndexes;
            refreshMaxTimestamp(prependToIndexes);
            version++;
        }
        stats.debt.add(indexLengthChange);
        return true;
    }

    private void refreshMaxTimestamp(ReadOnlyIndex[] concurrentReadableIndexs) {

        long maxTimestamp = -1;
        long maxTimestampVersion = -1;

        for (ReadOnlyIndex rawConcurrentReadableIndex : concurrentReadableIndexs) {
            Footer other = rawConcurrentReadableIndex.footer();
            if (rawhide.isNewerThan(other.maxTimestamp,
                other.maxTimestampVersion,
                maxTimestamp,
                maxTimestampVersion)) {

                maxTimestamp = other.maxTimestamp;
                maxTimestampVersion = other.maxTimestampVersion;
            }
        }

        maxTimestampAndVersion = new TimestampAndVersion(maxTimestamp, maxTimestampVersion);
    }

    public TimestampAndVersion maxTimeStampAndVersion() {
        return maxTimestampAndVersion;
    }

    public int debt() {
        if (disposed) {
            return 0;
        }
        int debt = (merging.length - 1);
        return debt < 0 ? 0 : debt;
    }


    public long count() throws Exception {
        long count = 0;
        for (ReadOnlyIndex g : grab()) {
            count += g.count();
        }
        return count;
    }

    public void close() throws Exception {
        synchronized (indexesLock) {
            for (ReadOnlyIndex index : indexes) {
                index.closeReadable();
            }
            closed = true;
        }
    }

    public void destroy() {
        synchronized (indexesLock) {
            for (ReadOnlyIndex index : indexes) {
                index.destroy();
            }
            closed = true;
        }
    }

    public Callable<Void> compactor(
        LABStats stats,
        String rawhideName,
        long splittableIfKeysLargerThanBytes,
        long splittableIfValuesLargerThanBytes,
        long splittableIfLargerThanBytes,
        SplitterBuilder splitterBuilder,
        int minMergeDebt,
        boolean fsync,
        MergerBuilder mergerBuilder
    ) throws Exception {
        if (disposed) {
            return null;
        }
        if (!splittable(splittableIfKeysLargerThanBytes, splittableIfValuesLargerThanBytes, splittableIfLargerThanBytes) && debt() == 0) {
            return null;
        }

        if (!compacting.compareAndSet(false, true)) {
            return null;
        }

        return () -> {
            try {

                if (splittable(splittableIfKeysLargerThanBytes, splittableIfValuesLargerThanBytes, splittableIfLargerThanBytes)) {
                    Callable<Void> splitter = splitterBuilder.buildSplitter(rawhideName, fsync, this::buildSplitter);
                    if (splitter != null) {
                        stats.spliting.incrementAndGet();
                        try {
                            splitter.call();
                            stats.split.incrementAndGet();
                        } finally {
                            stats.spliting.decrementAndGet();
                        }
                    }
                }

                if (debt() > 0) {
                    Callable<Void> merger = mergerBuilder.build(rawhideName, minMergeDebt, fsync, this::buildMerger);
                    if (merger != null) {
                        stats.merging.incrementAndGet();
                        try {
                            merger.call();
                            stats.merged.incrementAndGet();
                        } finally {
                            stats.merging.decrementAndGet();
                        }
                    }
                }

                return null;
            } finally {
                compacting.set(false);
            }
        };
    }

    private boolean splittable(
        long splittableIfKeysLargerThanBytes,
        long splittableIfValuesLargerThanBytes,
        long splittableIfLargerThanBytes) {

        ReadOnlyIndex[] splittable;
        synchronized (indexesLock) {
            if (disposed || indexes.length == 0) {
                return false;
            }
            splittable = indexes;
        }
        Comparator<byte[]> byteBufferKeyComparator = rawhide.getKeyComparator();
        byte[] minKey = null;
        byte[] maxKey = null;
        long worstCaseKeysSizeInBytes = 0;
        long worstCaseValuesSizeInBytes = 0;
        long worstCaseSizeInBytes = 0;

        for (ReadOnlyIndex aSplittable : splittable) {
            worstCaseKeysSizeInBytes += aSplittable.keysSizeInBytes();
            worstCaseValuesSizeInBytes += aSplittable.valuesSizeInBytes();
            worstCaseSizeInBytes += aSplittable.sizeInBytes();
            if (minKey == null) {
                minKey = aSplittable.minKey();
            } else {
                minKey = byteBufferKeyComparator.compare(minKey, aSplittable.minKey()) < 0 ? minKey : aSplittable.minKey();
            }

            if (maxKey == null) {
                maxKey = aSplittable.maxKey();
            } else {
                maxKey = byteBufferKeyComparator.compare(maxKey, aSplittable.maxKey()) < 0 ? maxKey : aSplittable.maxKey();
            }
        }

        if (Arrays.equals(minKey, maxKey)) {
            return false;
        }

        if (splittableIfLargerThanBytes > 0 && worstCaseSizeInBytes > splittableIfLargerThanBytes) {
            return true;
        }
        if (splittableIfKeysLargerThanBytes > 0 && worstCaseKeysSizeInBytes > splittableIfKeysLargerThanBytes) {
            return true;
        }
        return splittableIfValuesLargerThanBytes > 0 && worstCaseValuesSizeInBytes > splittableIfValuesLargerThanBytes;
    }

    private Splitter buildSplitter(IndexFactory leftHalfIndexFactory,
        IndexFactory rightHalfIndexFactory,
        CommitIndex commitIndex,
        boolean fsync) throws Exception {

        // lock out merging if possible
        long allVersion;
        ReadOnlyIndex[] all;
        synchronized (indexesLock) {
            allVersion = version;
            for (boolean b : merging) {
                if (b) {
                    return null;
                }
            }
            Arrays.fill(merging, true);
            all = indexes;
        }
        return new Splitter(all, allVersion, leftHalfIndexFactory, rightHalfIndexFactory, commitIndex, fsync);

    }

    public class Splitter implements Callable<Void> {

        private final ReadOnlyIndex[] all;
        private long allVersion;
        private final IndexFactory leftHalfIndexFactory;
        private final IndexFactory rightHalfIndexFactory;
        private final CommitIndex commitIndex;
        private final boolean fsync;

        public Splitter(ReadOnlyIndex[] all,
            long allVersion,
            IndexFactory leftHalfIndexFactory,
            IndexFactory rightHalfIndexFactory,
            CommitIndex commitIndex,
            boolean fsync) {

            this.all = all;
            this.allVersion = allVersion;
            this.leftHalfIndexFactory = leftHalfIndexFactory;
            this.rightHalfIndexFactory = rightHalfIndexFactory;
            this.commitIndex = commitIndex;
            this.fsync = fsync;
        }

        @Override
        public Void call() throws Exception {
            BolBuffer leftKeyBuffer = new BolBuffer();
            BolBuffer rightKeyBuffer = new BolBuffer();
            Comparator<byte[]> comparator = rawhide.getKeyComparator();
            while (true) {
                ReadIndex[] readers = new ReadIndex[all.length];
                try {
                    int splitLength = all.length;
                    long worstCaseCount = 0;
                    IndexRangeId join = null;
                    byte[] minKey = null;
                    byte[] maxKey = null;
                    for (int i = 0; i < all.length; i++) {
                        readers[i] = all[i].acquireReader();
                        worstCaseCount += readers[i].count();
                        IndexRangeId id = all[i].id();
                        if (join == null) {
                            join = new IndexRangeId(id.start, id.end, id.generation + 1);
                        } else {
                            join = join.join(id, Math.max(join.generation, id.generation + 1));
                        }

                        if (minKey == null) {
                            minKey = all[i].minKey();
                        } else {
                            minKey = comparator.compare(minKey, all[i].minKey()) < 0 ? minKey : all[i].minKey();
                        }

                        if (maxKey == null) {
                            maxKey = all[i].maxKey();
                        } else {
                            maxKey = comparator.compare(maxKey, all[i].maxKey()) < 0 ? maxKey : all[i].maxKey();
                        }
                    }

                    if (Arrays.equals(minKey, maxKey)) {
                        // TODO how not to get here over an over again when a key is larger that split size in byte Cannot split a single key
                        LOG.warn("Trying to split a single key." + Arrays.toString(minKey));
                        return null;
                    } else {
                        BolBuffer entryKeyBuffer = new BolBuffer();
                        byte[] middle = Lists.newArrayList(UIO.iterateOnSplits(minKey, maxKey, true, 1, rawhide.getKeyComparator())).get(1);
                        BolBuffer bbMiddle = new BolBuffer(middle);
                        LABAppendableIndex leftAppendableIndex = null;
                        LABAppendableIndex rightAppendableIndex = null;
                        try {
                            leftAppendableIndex = leftHalfIndexFactory.createIndex(join, worstCaseCount - 1);
                            rightAppendableIndex = rightHalfIndexFactory.createIndex(join, worstCaseCount - 1);
                            LABAppendableIndex effectiveFinalRightAppenableIndex = rightAppendableIndex;
                            InterleaveStream feedInterleaver = new InterleaveStream(rawhide,
                                ActiveScan.indexToFeeds(readers, false, false, null, null, rawhide, null));
                            try {
                                LOG.debug("Splitting with a middle of:{}", Arrays.toString(middle));

                                leftAppendableIndex.append((leftStream) -> {
                                    return effectiveFinalRightAppenableIndex.append((rightStream) -> {

                                        BolBuffer rawEntry = new BolBuffer();
                                        while ((rawEntry = feedInterleaver.next(rawEntry, null)) != null) {
                                            int c = rawhide.compareKey(rawEntry, entryKeyBuffer,
                                                bbMiddle);

                                            if (c < 0) {
                                                if (!leftStream.stream(rawEntry)) {
                                                    return false;
                                                }
                                            } else if (!rightStream.stream(rawEntry)) {
                                                return false;
                                            }
                                        }
                                        return true;
                                    }, rightKeyBuffer);
                                }, leftKeyBuffer);
                            } finally {
                                feedInterleaver.close();
                            }

                            LOG.debug("Splitting is flushing for a middle of:{}", Arrays.toString(middle));
                            if (leftAppendableIndex.getCount() > 0) {
                                leftAppendableIndex.closeAppendable(fsync);
                            } else {
                                leftAppendableIndex.delete();
                            }
                            if (rightAppendableIndex.getCount() > 0) {
                                rightAppendableIndex.closeAppendable(fsync);
                            } else {
                                rightAppendableIndex.delete();
                            }
                        } catch (Exception x) {
                            try {
                                if (leftAppendableIndex != null) {
                                    leftAppendableIndex.close();
                                    leftAppendableIndex.delete();
                                }
                                if (rightAppendableIndex != null) {
                                    rightAppendableIndex.close();
                                    rightAppendableIndex.delete();
                                }
                            } catch (Exception xx) {
                                LOG.error("Failed while trying to cleanup after a failure.", xx);
                            }
                            throw x;
                        }

                        List<IndexRangeId> commitRanges = new ArrayList<>();
                        commitRanges.add(join);

                        LOG.debug("Splitting trying to catchup for a middle of:{}", Arrays.toString(middle));
                        CATCHUP_YOU_BABY_TOMATO:
                        while (true) {
                            ReadOnlyIndex[] catchupMergeSet;
                            synchronized (indexesLock) {
                                if (allVersion == version) {
                                    LOG.debug("Commiting split for a middle of:{}", Arrays.toString(middle));

                                    commitIndex.commit(commitRanges);
                                    disposed = true;
                                    for (ReadOnlyIndex destroy : all) {
                                        destroy.destroy();
                                    }
                                    stats.debt.add(-indexes.length);
                                    indexes = new ReadOnlyIndex[0]; // TODO go handle null so that thread wait rety higher up
                                    refreshMaxTimestamp(indexes);
                                    version++;
                                    merging = new boolean[0];
                                    LOG.debug("All done splitting :) for a middle of:{}", Arrays.toString(middle));
                                    return null;
                                } else {

                                    LOG.debug("Version has changed {} for a middle of:{}", allVersion, Arrays.toString(middle));
                                    int catchupLength = merging.length - splitLength;
                                    for (int i = 0; i < catchupLength; i++) {
                                        if (merging[i]) {
                                            LOG.debug("Waiting for merge flag to clear at {} for a middle of:{}", i, Arrays.toString(middle));
                                            LOG.debug("splitLength={} merge.length={} catchupLength={}", splitLength, merging.length, catchupLength);
                                            LOG.debug("merging:{}", Arrays.toString(merging));
                                            indexesLock.wait();
                                            LOG.debug("Merge flag to cleared at {} for a middle of:{}", i, Arrays.toString(middle));
                                            continue CATCHUP_YOU_BABY_TOMATO;
                                        }
                                    }
                                    allVersion = version;
                                    catchupMergeSet = new ReadOnlyIndex[catchupLength];
                                    Arrays.fill(merging, 0, catchupLength, true);
                                    System.arraycopy(indexes, 0, catchupMergeSet, 0, catchupLength);
                                    splitLength = merging.length;
                                }
                            }

                            for (ReadOnlyIndex catchup : catchupMergeSet) {
                                IndexRangeId id = catchup.id();

                                LABAppendableIndex catchupLeftAppendableIndex = null;
                                LABAppendableIndex catchupRightAppendableIndex = null;
                                try {
                                    catchupLeftAppendableIndex = leftHalfIndexFactory.createIndex(id, catchup.count());
                                    catchupRightAppendableIndex = rightHalfIndexFactory.createIndex(id, catchup.count());
                                    LABAppendableIndex effectivelyFinalCatchupRightAppendableIndex = catchupRightAppendableIndex;

                                    ReadIndex catchupReader = catchup.acquireReader();
                                    try {
                                        InterleaveStream catchupFeedInterleaver = new InterleaveStream(rawhide,
                                            ActiveScan.indexToFeeds(new ReadIndex[] { catchup }, false, false, null, null, rawhide, null));
                                        try {
                                            LOG.debug("Doing a catchup split for a middle of:{}", Arrays.toString(middle));
                                            catchupLeftAppendableIndex.append((leftStream) -> {
                                                return effectivelyFinalCatchupRightAppendableIndex.append((rightStream) -> {

                                                    BolBuffer rawEntry = new BolBuffer();
                                                    while ((rawEntry = catchupFeedInterleaver.next(rawEntry, null)) != null) {
                                                        if (rawhide.compareKey(
                                                            rawEntry,
                                                            entryKeyBuffer,
                                                            bbMiddle) < 0) {
                                                            if (!leftStream.stream(rawEntry)) {
                                                                return false;
                                                            }
                                                        } else if (!rightStream.stream(rawEntry)) {
                                                            return false;
                                                        }
                                                    }
                                                    return true;
                                                }, rightKeyBuffer);
                                            }, leftKeyBuffer);
                                        } finally {
                                            catchupFeedInterleaver.close();
                                        }
                                    } finally {
                                        catchupReader.release();
                                    }
                                    LOG.debug("Catchup splitting is flushing for a middle of:{}", Arrays.toString(middle));
                                    catchupLeftAppendableIndex.closeAppendable(fsync);
                                    catchupRightAppendableIndex.closeAppendable(fsync);

                                    commitRanges.add(0, id);

                                } catch (Exception x) {
                                    try {
                                        if (catchupLeftAppendableIndex != null) {
                                            catchupLeftAppendableIndex.close();
                                            catchupLeftAppendableIndex.delete();
                                        }
                                        if (catchupRightAppendableIndex != null) {
                                            catchupRightAppendableIndex.close();
                                            catchupRightAppendableIndex.delete();
                                        }
                                    } catch (Exception xx) {
                                        LOG.error("Failed while trying to cleanup after a failure.", xx);
                                    }
                                    throw x;
                                }
                            }
                        }
                    }

                } catch (Exception x) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("[");
                    for (int i = 0; i < all.length; i++) {
                        if (i > 0) {
                            sb.append(", ");
                        }
                        sb.append(all[i].name());
                    }
                    sb.append("]");
                    LOG.error("Failed to split:" + allVersion + " for " + sb.toString(), x);
                    synchronized (indexesLock) {
                        Arrays.fill(merging, false);
                    }
                    throw x;
                } finally {
                    for (ReadIndex reader : readers) {
                        if (reader != null) {
                            reader.release();
                        }
                    }
                }
            }
        }
    }

    private Merger buildMerger(int minimumRun, boolean fsync, IndexFactory indexFactory, CommitIndex commitIndex) throws Exception {
        boolean[] mergingCopy;
        ReadOnlyIndex[] indexesCopy;
        ReadOnlyIndex[] mergeSet;
        MergeRange mergeRange;
        long[] counts;
        long[] sizes;
        long[] generations;
        synchronized (indexesLock) { // prevent others from trying to merge the same things
            if (indexes == null || indexes.length <= 1) {
                return null;
            }

            mergingCopy = merging;
            indexesCopy = indexes;

            counts = new long[indexesCopy.length];
            sizes = new long[indexesCopy.length];
            generations = new long[indexesCopy.length];

            for (int i = 0; i < counts.length; i++) {
                counts[i] = indexesCopy[i].count();
                generations[i] = indexesCopy[i].id().generation;
                sizes[i] = indexesCopy[i].sizeInBytes();
            }

            mergeRange = TieredCompaction.hbaseSause(minimumRun, mergingCopy, counts, sizes, generations);
            if (mergeRange == null) {
                return null;
            }

            mergeSet = new ReadOnlyIndex[mergeRange.length];
            System.arraycopy(indexesCopy, mergeRange.offset, mergeSet, 0, mergeRange.length);

            boolean[] updateMerging = new boolean[merging.length];
            System.arraycopy(merging, 0, updateMerging, 0, merging.length);
            Arrays.fill(updateMerging, mergeRange.offset, mergeRange.offset + mergeRange.length, true);
            merging = updateMerging;
        }

        IndexRangeId join = null;
        for (ReadOnlyIndex m : mergeSet) {
            IndexRangeId id = m.id();
            if (join == null) {
                join = new IndexRangeId(id.start, id.end, mergeRange.generation + 1);
            } else {
                join = join.join(id, Math.max(join.generation, id.generation));
            }
        }

        return new Merger(counts, generations, mergeSet, join, indexFactory, commitIndex, fsync, mergeRange);
    }

    public class Merger implements Callable<Void> {

        private final long[] counts;
        private final long[] generations;
        private final ReadOnlyIndex[] mergeSet;
        private final IndexRangeId mergeRangeId;
        private final IndexFactory indexFactory;
        private final CommitIndex commitIndex;
        private final boolean fsync;
        private final MergeRange mergeRange;

        private Merger(
            long[] counts,
            long[] generations,
            ReadOnlyIndex[] mergeSet,
            IndexRangeId mergeRangeId,
            IndexFactory indexFactory,
            CommitIndex commitIndex,
            boolean fsync,
            MergeRange mergeRange) {

            this.mergeRange = mergeRange;
            this.counts = counts;
            this.generations = generations;
            this.mergeSet = mergeSet;
            this.mergeRangeId = mergeRangeId;
            this.indexFactory = indexFactory;
            this.commitIndex = commitIndex;
            this.fsync = fsync;
        }

        @Override
        public String toString() {
            return "Merger{" + "mergeRangeId=" + mergeRangeId + '}';
        }

        @Override
        public Void call() throws Exception {
            BolBuffer keyBuffer = new BolBuffer();
            ReadOnlyIndex index;
            ReadIndex[] readers = new ReadIndex[mergeSet.length];
            try {

                long startMerge = System.currentTimeMillis();

                long worstCaseCount = 0;
                for (int i = 0; i < mergeSet.length; i++) {
                    readers[i] = mergeSet[i].acquireReader();
                    worstCaseCount += readers[i].count();
                }

                LABAppendableIndex appendableIndex = null;
                try {
                    appendableIndex = indexFactory.createIndex(mergeRangeId, worstCaseCount);
                    InterleaveStream feedInterleaver = new InterleaveStream(rawhide,
                        ActiveScan.indexToFeeds(readers, false, false, null, null, rawhide, null));
                    try {
                        appendableIndex.append((stream) -> {

                            BolBuffer rawEntry = new BolBuffer();
                            while ((rawEntry = feedInterleaver.next(rawEntry, null)) != null) {
                                if (!stream.stream(rawEntry)) {
                                    return false;
                                }
                            }
                            return true;

                        }, keyBuffer);
                    } finally {
                        feedInterleaver.close();
                    }
                    appendableIndex.closeAppendable(fsync);
                } catch (Exception x) {
                    try {
                        if (appendableIndex != null) {
                            appendableIndex.close();
                            appendableIndex.delete();
                        }
                    } catch (Exception xx) {
                        LOG.error("Failed while trying to cleanup after a failure.", xx);
                    }
                    throw x;
                }

                index = commitIndex.commit(Collections.singletonList(mergeRangeId));


                int indexLengthChange;
                synchronized (indexesLock) {
                    int newLength = (indexes.length - mergeSet.length) + 1;
                    boolean[] updateMerging = new boolean[newLength];
                    ReadOnlyIndex[] updateIndexes = new ReadOnlyIndex[newLength];

                    int ui = 0;
                    int mi = 0;
                    for (int i = 0; i < indexes.length; i++) {
                        if (mi < mergeSet.length && indexes[i] == mergeSet[mi]) {
                            if (mi == 0) {
                                updateMerging[ui] = false;
                                updateIndexes[ui] = index;
                                ui++;
                            }
                            mi++;
                        } else {
                            updateMerging[ui] = merging[i];
                            updateIndexes[ui] = indexes[i];
                            ui++;
                        }
                    }

                    merging = updateMerging;
                    indexLengthChange = updateIndexes.length - indexes.length;
                    indexes = updateIndexes;
                    refreshMaxTimestamp(updateIndexes);
                    version++;
                }
                stats.debt.add(indexLengthChange);

                LOG.debug("Merged:  {} millis counts:{} gens:{} {}",
                    (System.currentTimeMillis() - startMerge),
                    TieredCompaction.range(counts, mergeRange.offset, mergeRange.length),
                    Arrays.toString(generations),
                    index.name()
                );

                for (ReadOnlyIndex rawConcurrentReadableIndex : mergeSet) {
                    rawConcurrentReadableIndex.destroy();
                }

            } catch (Exception x) {
                StringBuilder sb = new StringBuilder();
                sb.append("[");
                for (int i = 0; i < mergeSet.length; i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    sb.append(mergeSet[i].name());
                }
                sb.append("]");
                LOG.error("Failed to merge range:" + mergeRangeId + " for " + sb.toString(), x);

                synchronized (indexesLock) {
                    boolean[] updateMerging = new boolean[merging.length];
                    int mi = 0;
                    for (int i = 0; i < indexes.length; i++) {
                        if (mi < mergeSet.length && indexes[i] == mergeSet[mi]) {
                            updateMerging[i] = false;
                            mi++;
                        } else {
                            updateMerging[i] = merging[i];
                        }
                    }
                    merging = updateMerging;
                }
            } finally {
                for (ReadIndex reader : readers) {
                    if (reader != null) {
                        reader.release();
                    }
                }
            }

            return null;
        }

    }

    public boolean tx(int index, boolean pointFrom, byte[] fromKey, byte[] toKey, ReaderTx tx, boolean hydrateValues) throws Exception {

        ReadOnlyIndex[] stackIndexes;

        ReadIndex[] readIndexs;
        START_OVER:
        while (true) {
            synchronized (indexesLock) {
                if (disposed) {
                    throw new LABConcurrentSplitException();
                }
                if (closed) {
                    throw new LABClosedException("");
                }
                stackIndexes = indexes;
            }
            readIndexs = new ReadIndex[stackIndexes.length];
            try {
                for (int i = 0; i < readIndexs.length; i++) {
                    readIndexs[i] = stackIndexes[i].acquireReader();
                    if (readIndexs[i] == null) {
                        releaseReaders(readIndexs);
                        continue START_OVER;
                    }
                }
            } catch (Exception x) {
                releaseReaders(readIndexs);
                throw x;
            }
            break;
        }

        try {
            return tx.tx(index, pointFrom, fromKey, toKey, readIndexs, hydrateValues);
        } finally {
            releaseReaders(readIndexs);
        }
    }

    private void releaseReaders(ReadIndex[] readIndexs) {
        for (int i = 0; i < readIndexs.length; i++) {
            if (readIndexs[i] != null) {
                readIndexs[i].release();
                readIndexs[i] = null;
            }
        }
    }

    private ReadOnlyIndex[] grab() {
        ReadOnlyIndex[] copy;
        synchronized (indexesLock) {
            copy = indexes;
        }
        return copy;
    }

    void auditRanges(String prefix, KeyToString keyToString) {
        ReadOnlyIndex[] copy;
        synchronized (indexesLock) {
            copy = indexes;
        }
        for (ReadOnlyIndex aCopy : copy) {
            System.out.println(prefix + keyToString.keyToString(aCopy.minKey()) + "->" + keyToString.keyToString(aCopy.maxKey()));
        }
    }

}
