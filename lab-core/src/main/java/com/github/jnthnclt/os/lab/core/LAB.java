package com.github.jnthnclt.os.lab.core;

import com.github.jnthnclt.os.lab.collections.bah.LRUConcurrentBAHLinkedHash;
import com.github.jnthnclt.os.lab.core.api.AppendValues;
import com.github.jnthnclt.os.lab.core.api.Keys;
import com.github.jnthnclt.os.lab.core.api.Ranges;
import com.github.jnthnclt.os.lab.core.api.ScanKeys;
import com.github.jnthnclt.os.lab.core.api.Snapshot;
import com.github.jnthnclt.os.lab.core.api.ValueIndex;
import com.github.jnthnclt.os.lab.core.api.ValueStream;
import com.github.jnthnclt.os.lab.core.api.exceptions.LABClosedException;
import com.github.jnthnclt.os.lab.core.api.exceptions.LABCorruptedException;
import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.guts.ActiveScan;
import com.github.jnthnclt.os.lab.core.guts.InterleaveStream;
import com.github.jnthnclt.os.lab.core.guts.InterleavingStreamFeed;
import com.github.jnthnclt.os.lab.core.guts.LABHashIndexType;
import com.github.jnthnclt.os.lab.core.guts.LABIndex;
import com.github.jnthnclt.os.lab.core.guts.LABIndexProvider;
import com.github.jnthnclt.os.lab.core.guts.LABMemoryIndex;
import com.github.jnthnclt.os.lab.core.guts.Leaps;
import com.github.jnthnclt.os.lab.core.guts.RangeStripedCompactableIndexes;
import com.github.jnthnclt.os.lab.core.guts.ReaderTx;
import com.github.jnthnclt.os.lab.core.guts.api.KeyToString;
import com.github.jnthnclt.os.lab.core.guts.api.ReadIndex;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import com.github.jnthnclt.os.lab.core.util.LABLogger;
import com.github.jnthnclt.os.lab.core.util.LABLoggerFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class LAB implements ValueIndex<byte[]> {


    static private class CompactLock {
    }

    private static final LABLogger LOG = LABLoggerFactory.getLogger();
    ;

    private final static byte[] SMALLEST_POSSIBLE_KEY = new byte[0];

    private final ExecutorService schedule;
    private final ExecutorService compact;
    private final ExecutorService destroy;
    private final LABWAL wal;
    private final byte[] walId;
    private final AtomicLong walAppendVersion = new AtomicLong(0);

    private final LABHeapPressure LABHeapPressure;
    private final long maxHeapPressureInBytes;
    private final int minDebt;
    private final int maxDebt;
    private final String primaryName;
    private final RangeStripedCompactableIndexes rangeStripedCompactableIndexes;
    private final Semaphore commitSemaphore = new Semaphore(Short.MAX_VALUE, true);
    private final CompactLock compactLock = new CompactLock();
    private final AtomicLong ongoingCompactions = new AtomicLong();
    private final AtomicBoolean closeRequested = new AtomicBoolean(false);

    private volatile LABMemoryIndex memoryIndex;
    private volatile LABMemoryIndex flushingMemoryIndex;
    private volatile boolean corrupt = false;

    private final LABStats stats;
    private final String rawhideName;
    private final Rawhide rawhide;
    private final LABIndexProvider<BolBuffer, BolBuffer> indexProvider;
    private final boolean hashIndexEnabled;

    private volatile long lastAppendTimestamp = 0;
    private volatile long lastCommitTimestamp = System.currentTimeMillis();

    public LAB(LABStats stats,
        String rawhideName,
        Rawhide rawhide,
        ExecutorService schedule,
        ExecutorService compact,
        ExecutorService destroy,
        File root,
        LABWAL wal,
        byte[] walId,
        String primaryName,
        int entriesBetweenLeaps,
        LABHeapPressure LABHeapPressure,
        long maxHeapPressureInBytes,
        int minDebt,
        int maxDebt,
        long splitWhenKeysTotalExceedsNBytes,
        long splitWhenValuesTotalExceedsNBytes,
        long splitWhenValuesAndKeysTotalExceedsNBytes,
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache,
        LABIndexProvider<BolBuffer, BolBuffer> indexProvider,
        boolean fsyncFileRenames,
        LABHashIndexType hashIndexType,
        double hashIndexLoadFactor,
        boolean hashIndexEnabled,
        long deleteTombstonedVersionsAfterMillis) throws Exception {

        stats.open.increment();

        this.stats = stats;
        this.rawhideName = rawhideName;
        this.rawhide = rawhide;
        this.schedule = schedule;
        this.compact = compact;
        this.destroy = destroy;
        this.wal = wal;
        this.walId = walId;
        this.LABHeapPressure = LABHeapPressure;
        this.maxHeapPressureInBytes = maxHeapPressureInBytes;
        this.memoryIndex = new LABMemoryIndex(destroy, LABHeapPressure, stats, rawhide, indexProvider.create(rawhide, -1));
        this.primaryName = primaryName;
        this.rangeStripedCompactableIndexes = new RangeStripedCompactableIndexes(stats,
            destroy,
            root,
            primaryName,
            entriesBetweenLeaps,
            splitWhenKeysTotalExceedsNBytes,
            splitWhenValuesTotalExceedsNBytes,
            splitWhenValuesAndKeysTotalExceedsNBytes,
            rawhide,
            leapsCache,
            fsyncFileRenames,
            hashIndexType,
            hashIndexLoadFactor,
            deleteTombstonedVersionsAfterMillis);
        this.minDebt = minDebt;
        this.maxDebt = maxDebt;
        this.indexProvider = indexProvider;
        this.hashIndexEnabled = hashIndexEnabled;
    }

    @Override
    public String name() {
        return primaryName;
    }

    @Override
    public int debt() throws Exception {
        return rangeStripedCompactableIndexes.debt();
    }

    @Override
    public boolean get(Keys keys, ValueStream stream, boolean hydrateValues) throws Exception {
        BolBuffer streamKeyBuffer = new BolBuffer();
        BolBuffer streamValueBuffer = hydrateValues ? new BolBuffer() : null;

        boolean b = pointTx(keys,
            -1,
            -1,
            streamKeyBuffer,
            streamValueBuffer,
            stream,
            hydrateValues
        );
        stats.gets.increment();
        return b;
    }

    @Override
    public boolean rangeScan(byte[] from, byte[] to, ValueStream stream, boolean hydrateValues) throws Exception {
        BolBuffer streamKeyBuffer = new BolBuffer();
        BolBuffer streamValueBuffer = hydrateValues ? new BolBuffer() : null;

        boolean r = rangeTx(true, -1, from, to, -1, -1,
            (index, fromKey, toKey, readIndexes, hydrateValues1) -> {

                InterleaveStream interleaveStream = new InterleaveStream(rawhide,
                    ActiveScan.indexToFeeds(readIndexes, fromKey, toKey, rawhide, null));
                try {

                    while (true) {
                        BolBuffer next = interleaveStream.next(new BolBuffer(), null);
                        if (next == null) {
                            break;
                        }
                        if (!rawhide.streamRawEntry(index,
                            next,
                            streamKeyBuffer,
                            streamValueBuffer,
                            stream)) {
                            return false;
                        }
                    }
                    return true;
                } finally {
                    interleaveStream.close();
                }
            },
            hydrateValues
        );
        stats.rangeScan.increment();
        return r;
    }

    @Override
    public boolean rangesScan(Ranges ranges, ValueStream stream, boolean hydrateValues) throws Exception {
        BolBuffer streamKeyBuffer = new BolBuffer();
        BolBuffer streamValueBuffer = hydrateValues ? new BolBuffer() : null;
        boolean r = ranges.ranges((index, from, to) -> {
            return rangeTx(true, index, from, to, -1, -1,
                (index1, fromKey, toKey, readIndexes, hydrateValues1) -> {

                    InterleaveStream interleaveStream = new InterleaveStream(rawhide,
                        ActiveScan.indexToFeeds(readIndexes, fromKey, toKey, rawhide, null));
                    try {
                        while (true) {
                            BolBuffer next = interleaveStream.next(new BolBuffer(), null);
                            if (next == null) {
                                break;
                            }
                            if (!rawhide.streamRawEntry(index,
                                next,
                                streamKeyBuffer,
                                streamValueBuffer,
                                stream)) {
                                return false;
                            }
                        }
                        return true;
                    } finally {
                        interleaveStream.close();
                    }
                },
                hydrateValues
            );
        });
        stats.multiRangeScan.increment();
        return r;

    }

    @Override
    public boolean rowScan(ScanKeys keys, ValueStream stream, boolean hydrateValues) throws Exception {
        BolBuffer streamKeyBuffer = new BolBuffer();
        BolBuffer streamValueBuffer = hydrateValues ? new BolBuffer() : null;


        BolBuffer[] keyHint = new BolBuffer[1];
        keyHint[0] = keys.nextKey();

        BolBuffer[] lastNext = new BolBuffer[1];
        BolBuffer keyBuffer = new BolBuffer();


        boolean r = rangeTx(true,
            -1,
            SMALLEST_POSSIBLE_KEY,
            null,
            -1,
            -1,
            (index, fromKey, toKey, readIndexes, hydrateValues1) -> {

                AtomicBoolean eos = new AtomicBoolean();

                PriorityQueue<InterleavingStreamFeed> interleavingStreamFeeds = ActiveScan.indexToFeeds(readIndexes,
                    fromKey, toKey, rawhide, keyHint[0]);
                InterleaveStream interleaveStream = new InterleaveStream(rawhide, interleavingStreamFeeds);

                try {

                    while (true) {
                        if (keyHint[0] == null) {
                            return true;
                        }

                        if (lastNext[0] != null) {
                            int c = rawhide.compareKey(lastNext[0], keyBuffer, keyHint[0]);
                            if (c == 0) {
                                BolBuffer next = lastNext[0];
                                lastNext[0] = null;
                                if (!rawhide.streamRawEntry(index,
                                    next,
                                    streamKeyBuffer,
                                    streamValueBuffer,
                                    stream)) {
                                    return false;
                                }
                                keyHint[0] = keys.nextKey();
                                continue;
                            } else if (c > 0) {
                                if (!rawhide.streamRawEntry(index,
                                    null,
                                    streamKeyBuffer,
                                    streamValueBuffer,
                                    stream)) {
                                    return false;
                                }
                                keyHint[0] = keys.nextKey();
                                continue;
                            }
                        }

                        BolBuffer rawEntry = new BolBuffer();
                        BolBuffer next = interleaveStream.next(rawEntry, keyHint[0]);
                        if (next == null) {
                            return true;
                        }
                        if (rawhide.compareKey(next, keyBuffer, keyHint[0]) == 0) {
                            lastNext[0] = null;
                            if (!rawhide.streamRawEntry(index,
                                next,
                                streamKeyBuffer,
                                streamValueBuffer,
                                stream)) {
                                return false;
                            }
                        } else {
                            lastNext[0] = next;
                            if (!rawhide.streamRawEntry(index,
                                null,
                                streamKeyBuffer,
                                streamValueBuffer,
                                stream)) {
                                return false;
                            }
                        }
                        keyHint[0] = keys.nextKey();
                        continue;
                    }

                } finally {
                    if (interleaveStream != null) {
                        interleaveStream.close();
                    }
                }
            },
            hydrateValues
        );

        while (keyHint[0] != null) {
            if (!rawhide.streamRawEntry(-1,
                null,
                streamKeyBuffer,
                streamValueBuffer,
                stream)) {
                return false;
            }
            keyHint[0] = keys.nextKey();
        }

        stats.rowScan.increment();
        return r;
    }

    @Override
    public boolean rowScan(ValueStream stream, boolean hydrateValues) throws Exception {
        BolBuffer streamKeyBuffer = new BolBuffer();
        BolBuffer streamValueBuffer = hydrateValues ? new BolBuffer() : null;
        boolean r = rangeTx(true,
            -1,
            SMALLEST_POSSIBLE_KEY,
            null,
            -1,
            -1,
            (index, fromKey, toKey, readIndexes, hydrateValues1) -> {

                PriorityQueue<InterleavingStreamFeed> interleavingStreamFeeds = ActiveScan.indexToFeeds(readIndexes, fromKey, toKey, rawhide, null);
                InterleaveStream interleaveStream = new InterleaveStream(rawhide, interleavingStreamFeeds);
                try {
                    BolBuffer rawEntry = new BolBuffer();
                    while (true) {
                        BolBuffer next = interleaveStream.next(rawEntry, null);
                        if (next == null) {
                            return true;
                        }
                        if (!rawhide.streamRawEntry(index,
                            next,
                            streamKeyBuffer,
                            streamValueBuffer,
                            stream)) {
                            return false;
                        }
                    }
                } finally {
                    interleaveStream.close();
                }
            },
            hydrateValues
        );
        stats.rowScan.increment();
        return r;
    }

    @Override
    public long count() throws Exception {
        return memoryIndex.count() + rangeStripedCompactableIndexes.count();
    }

    @Override
    public boolean isEmpty() throws Exception {
        if (memoryIndex.isEmpty()) {
            return rangeStripedCompactableIndexes.isEmpty();
        }
        return false;
    }

    public long approximateHeapPressureInBytes() {
        LABMemoryIndex stackCopyFlushingMemoryIndex = flushingMemoryIndex;
        LABMemoryIndex stackCopyMemoryIndex = memoryIndex;

        return memoryIndex.sizeInBytes()
            + ((stackCopyFlushingMemoryIndex != null && stackCopyFlushingMemoryIndex != stackCopyMemoryIndex) ? stackCopyFlushingMemoryIndex.sizeInBytes() : 0);
    }

    public long lastAppendTimestamp() {
        return lastAppendTimestamp;
    }

    public long lastCommitTimestamp() {
        return lastCommitTimestamp;
    }

    private boolean pointTx(Keys keys,
        long newerThanTimestamp,
        long newerThanTimestampVersion,
        BolBuffer streamKeyBuffer,
        BolBuffer streamValueBuffer,
        ValueStream valueStream,
        boolean hydrateValues) throws Exception {

        ReadIndex memoryIndexReader = null;
        ReadIndex flushingMemoryIndexReader = null;
        try {
            while (!closeRequested.get()) {
                LABMemoryIndex memoryIndexStackCopy;
                LABMemoryIndex flushingMemoryIndexStackCopy;

                commitSemaphore.acquire();
                try {
                    memoryIndexStackCopy = memoryIndex;
                    flushingMemoryIndexStackCopy = flushingMemoryIndex;
                } finally {
                    commitSemaphore.release();
                }

                if (memoryIndexStackCopy.mightContain(newerThanTimestamp, newerThanTimestampVersion)) {

                    memoryIndexReader = memoryIndexStackCopy.acquireReader();
                    if (memoryIndexReader != null) {
                        if (flushingMemoryIndexStackCopy != null) {
                            if (flushingMemoryIndexStackCopy.mightContain(newerThanTimestamp, newerThanTimestampVersion)) {
                                flushingMemoryIndexReader = flushingMemoryIndexStackCopy.acquireReader();
                                if (flushingMemoryIndexReader != null) {
                                    break;
                                } else {
                                    memoryIndexReader.release();
                                    memoryIndexReader = null;
                                }
                            }
                        } else {
                            break;
                        }
                    }
                } else if (flushingMemoryIndexStackCopy != null) {
                    if (flushingMemoryIndexStackCopy.mightContain(newerThanTimestamp, newerThanTimestampVersion)) {
                        flushingMemoryIndexReader = flushingMemoryIndexStackCopy.acquireReader();
                        if (flushingMemoryIndexReader != null) {
                            break;
                        }
                    }
                } else {
                    break;
                }
            }

            if (closeRequested.get()) {
                throw new LABClosedException("");
            }

            return rangeStripedCompactableIndexes.pointTx(keys,
                newerThanTimestamp,
                newerThanTimestampVersion,
                memoryIndexReader,
                flushingMemoryIndexReader,
                hashIndexEnabled,
                hydrateValues,
                streamKeyBuffer,
                streamValueBuffer,
                valueStream);
        } finally {
            if (memoryIndexReader != null) {
                memoryIndexReader.release();
            }
            if (flushingMemoryIndexReader != null) {
                flushingMemoryIndexReader.release();
            }
        }
    }

    private boolean rangeTx(boolean acquireCommitSemaphore,
        int index,
        byte[] from,
        byte[] to,
        long newerThanTimestamp,
        long newerThanTimestampVersion,
        ReaderTx tx,
        boolean hydrateValues) throws Exception {

        ReadIndex memoryIndexReader = null;
        ReadIndex flushingMemoryIndexReader = null;
        try {
            while (true) {
                LABMemoryIndex memoryIndexStackCopy;
                LABMemoryIndex flushingMemoryIndexStackCopy;

                if (acquireCommitSemaphore) {
                    commitSemaphore.acquire();
                }
                try {
                    memoryIndexStackCopy = memoryIndex;
                    flushingMemoryIndexStackCopy = flushingMemoryIndex;
                } finally {
                    if (acquireCommitSemaphore) {
                        commitSemaphore.release();
                    }
                }

                if (memoryIndexStackCopy.mightContain(newerThanTimestamp, newerThanTimestampVersion)) {

                    memoryIndexReader = memoryIndexStackCopy.acquireReader();
                    if (memoryIndexReader != null) {
                        if (flushingMemoryIndexStackCopy != null) {
                            if (flushingMemoryIndexStackCopy.mightContain(newerThanTimestamp, newerThanTimestampVersion)) {
                                flushingMemoryIndexReader = flushingMemoryIndexStackCopy.acquireReader();
                                if (flushingMemoryIndexReader != null) {
                                    break;
                                } else {
                                    memoryIndexReader.release();
                                    memoryIndexReader = null;
                                }
                            }
                        } else {
                            break;
                        }
                    }
                } else if (flushingMemoryIndexStackCopy != null) {
                    if (flushingMemoryIndexStackCopy.mightContain(newerThanTimestamp, newerThanTimestampVersion)) {
                        flushingMemoryIndexReader = flushingMemoryIndexStackCopy.acquireReader();
                        if (flushingMemoryIndexReader != null) {
                            break;
                        }
                    }
                } else {
                    break;
                }

            }

            ReadIndex reader = memoryIndexReader;
            ReadIndex flushingReader = flushingMemoryIndexReader;
            return rangeStripedCompactableIndexes.rangeTx(index,
                from,
                to,
                newerThanTimestamp,
                newerThanTimestampVersion,
                (index1, fromKey, toKey, acquired, hydrateValues1) -> {
                    int active = (reader == null) ? 0 : 1;
                    int flushing = (flushingReader == null) ? 0 : 1;
                    ReadIndex[] indexes = new ReadIndex[acquired.length + active + flushing];
                    int i = 0;
                    if (reader != null) {
                        indexes[i] = reader;
                        i++;
                    }
                    if (flushingReader != null) {
                        indexes[i] = flushingReader;
                        i++;
                    }
                    System.arraycopy(acquired, 0, indexes, active + flushing, acquired.length);
                    return tx.tx(index1, fromKey, toKey, indexes, hydrateValues1);
                },
                hydrateValues
            );
        } finally {
            if (memoryIndexReader != null) {
                memoryIndexReader.release();
            }
            if (flushingMemoryIndexReader != null) {
                flushingMemoryIndexReader.release();
            }
        }

    }

    @Override
    public boolean append(AppendValues<byte[]> values, boolean fsyncOnFlush,
        BolBuffer rawEntryBuffer, BolBuffer keyBuffer) throws Exception {
        return internalAppend(values, fsyncOnFlush, -1, rawEntryBuffer, keyBuffer, true);
    }

    public boolean onOpenAppend(AppendValues<byte[]> values, boolean fsyncOnFlush, long overrideMaxHeapPressureInBytes,
        BolBuffer rawEntryBuffer, BolBuffer keyBuffer) throws Exception {
        return internalAppend(values, fsyncOnFlush, overrideMaxHeapPressureInBytes, rawEntryBuffer, keyBuffer, false);
    }

    private boolean internalAppend(
        AppendValues<byte[]> values,
        boolean fsyncOnFlush,
        long overrideMaxHeapPressureInBytes,
        BolBuffer rawEntryBuffer,
        BolBuffer keyBuffer,
        boolean journal) throws Exception {

        if (values == null) {
            return false;
        }

        boolean[] appended = { false };
        commitSemaphore.acquire();
        try {
            if (closeRequested.get()) {
                throw new LABClosedException("");
            }

            long appendVersion;
            if (journal && wal != null) {
                appendVersion = walAppendVersion.incrementAndGet();
            } else {
                appendVersion = -1;
            }
            lastAppendTimestamp = System.currentTimeMillis();

            long[] count = { 0 };
            if (journal && wal != null) {
                wal.appendTx(walId, appendVersion, fsyncOnFlush, activeWAL -> {
                    appended[0] = memoryIndex.append(
                        (stream) -> {
                            return values.consume(
                                (index, key, timestamp, tombstoned, version, value) -> {

                                    BolBuffer rawEntry = rawhide.toRawEntry(key, timestamp, tombstoned, version, value, rawEntryBuffer);
                                    activeWAL.append(walId, appendVersion, rawEntry);
                                    stats.journaledAppend.increment();

                                    count[0]++;
                                    return stream.stream(rawEntry);
                                }
                            );
                        }, keyBuffer
                    );
                });
            } else {
                appended[0] = memoryIndex.append(
                    (stream) -> {
                        return values.consume(
                            (index, key, timestamp, tombstoned, version, value) -> {

                                BolBuffer rawEntry = rawhide.toRawEntry(key, timestamp, tombstoned, version, value, rawEntryBuffer);
                                stats.append.increment();
                                count[0]++;
                                return stream.stream(rawEntry);
                            }
                        );
                    }, keyBuffer
                );
            }
            LOG.inc("append>count", count[0]);

        } finally {
            commitSemaphore.release();
        }
        LABHeapPressure.commitIfNecessary(this, overrideMaxHeapPressureInBytes >= 0 ? overrideMaxHeapPressureInBytes : maxHeapPressureInBytes, fsyncOnFlush);
        return appended[0];
    }

    @Override
    public List<Future<Object>> commit(boolean fsync, boolean waitIfToFarBehind) throws Exception {
        if (memoryIndex.isEmpty()) {
            return Collections.emptyList();
        }

        if (corrupt) {
            throw new LABCorruptedException();
        }
        if (closeRequested.get()) {
            throw new LABClosedException("");
        }

        if (!internalCommit(fsync, new BolBuffer(), new BolBuffer(), new BolBuffer())) { // grr
            return Collections.emptyList();
        }
        if (waitIfToFarBehind) {
            return compact(fsync, minDebt, maxDebt, true);
        } else {
            return Collections.singletonList(schedule.submit(() -> compact(fsync, minDebt, maxDebt, false)));
        }
    }

    private boolean internalCommit(boolean fsync, BolBuffer keyBuffer, BolBuffer entryBuffer, BolBuffer entryKeyBuffer) throws Exception {
        synchronized (commitSemaphore) {
            long appendVersion = -1;
            // open new memory index and mark existing for flush
            commitSemaphore.acquire(Short.MAX_VALUE);
            try {
                if (memoryIndex.isEmpty()) {
                    return false;
                }
                if (fsync) {
                    stats.fsyncedCommit.increment();
                } else {
                    stats.commit.increment();
                }
                if (wal != null) {
                    appendVersion = walAppendVersion.incrementAndGet();
                }
                flushingMemoryIndex = memoryIndex;
                LABIndex<BolBuffer, BolBuffer> labIndex = indexProvider.create(rawhide, flushingMemoryIndex.poweredUpTo());
                memoryIndex = new LABMemoryIndex(destroy, LABHeapPressure, stats, rawhide, labIndex);
            } finally {
                commitSemaphore.release(Short.MAX_VALUE);
            }

            // flush existing memory index to disk
            rangeStripedCompactableIndexes.append(rawhideName, flushingMemoryIndex, fsync, keyBuffer, entryBuffer, entryKeyBuffer);

            // destroy existing memory index
            LABMemoryIndex destroyableMemoryIndex;
            commitSemaphore.acquire(Short.MAX_VALUE);
            try {
                destroyableMemoryIndex = flushingMemoryIndex;
                flushingMemoryIndex = null;
            } finally {
                commitSemaphore.release(Short.MAX_VALUE);
            }
            destroyableMemoryIndex.destroy();

            // commit to WAL
            if (wal != null) {
                wal.commit(walId, appendVersion, fsync);
            }
            lastCommitTimestamp = System.currentTimeMillis();

            return true;
        }
    }

    public void snapshot(Snapshot snapshot) throws Exception {
        // TODO differ snapshot if compaction in progress and avoid compacting when snapshot in progress
        rangeStripedCompactableIndexes.snapshot(snapshot);
    }

    @Override
    public List<Future<Object>> compact(boolean fsync, int minDebt, int maxDebt, boolean waitIfToFarBehind) throws Exception {

        int debt = rangeStripedCompactableIndexes.debt();
        if (debt == 0 || debt < minDebt) {
            return Collections.emptyList();
        }

        List<Future<Object>> awaitable = null;
        while (!closeRequested.get()) {
            if (corrupt) {
                throw new LABCorruptedException();
            }
            List<Callable<Void>> compactors = rangeStripedCompactableIndexes.buildCompactors(rawhideName, fsync, minDebt);
            if (compactors != null && !compactors.isEmpty()) {
                if (awaitable == null) {
                    awaitable = new ArrayList<>(compactors.size());
                }
                for (Callable<Void> compactor : compactors) {
                    LOG.debug("Scheduling async compaction:{} for index:{} debt:{}", compactors, rangeStripedCompactableIndexes, debt);
                    synchronized (compactLock) {
                        if (closeRequested.get()) {
                            break;
                        } else {
                            ongoingCompactions.incrementAndGet();
                        }
                    }
                    Future<Object> future = compact.submit(() -> {
                        try {
                            compactor.call();
                        } catch (Exception x) {
                            LOG.error("Failed to compact " + rangeStripedCompactableIndexes, x);
                            corrupt = true;
                        } finally {
                            synchronized (compactLock) {
                                ongoingCompactions.decrementAndGet();
                                compactLock.notifyAll();
                            }
                        }
                        return null;
                    });
                    awaitable.add(future);
                }
            }

            if (waitIfToFarBehind && debt >= maxDebt) {
                synchronized (compactLock) {
                    if (!closeRequested.get() && ongoingCompactions.get() > 0) {
                        LOG.debug("Waiting because debt is too high for index:{} debt:{}", rangeStripedCompactableIndexes, debt);
                        compactLock.wait();
                    } else {
                        break;
                    }
                }
                debt = rangeStripedCompactableIndexes.debt();
            } else {
                break;
            }
        }
        return awaitable;
    }

    @Override
    public void close(boolean flushUncommited, boolean fsync) throws Exception {
        LABHeapPressure.close(this);

        if (!closeRequested.compareAndSet(false, true)) {
            throw new LABClosedException("");
        }

        if (flushUncommited) {
            internalCommit(fsync, new BolBuffer(), new BolBuffer(), new BolBuffer()); // grr
        }

        synchronized (compactLock) {
            while (ongoingCompactions.get() > 0) {
                compactLock.wait();
            }
        }

        commitSemaphore.acquire(Short.MAX_VALUE);
        try {
            memoryIndex.closeReadable();
            rangeStripedCompactableIndexes.close();
            memoryIndex.destroy();
        } finally {
            commitSemaphore.release(Short.MAX_VALUE);
        }
        LOG.debug("Closed {}", this);
        stats.closed.increment();

    }

    @Override
    public String toString() {
        return "LAB{"
            + ", minDebt=" + minDebt
            + ", maxDebt=" + maxDebt
            + ", ongoingCompactions=" + ongoingCompactions
            + ", corrupt=" + corrupt
            + ", rawhide=" + rawhide
            + '}';
    }


    public void auditRanges(KeyToString keyToString) throws Exception {
        rangeStripedCompactableIndexes.auditRanges(keyToString);
    }

}
