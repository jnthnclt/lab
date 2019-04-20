package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.base.BolBuffer;
import com.github.jnthnclt.os.lab.base.UIO;
import com.github.jnthnclt.os.lab.collections.bah.LRUConcurrentBAHLinkedHash;
import com.github.jnthnclt.os.lab.core.LABStats;
import com.github.jnthnclt.os.lab.core.api.Keys;
import com.github.jnthnclt.os.lab.core.api.ValueStream;
import com.github.jnthnclt.os.lab.core.api.exceptions.LABConcurrentSplitException;
import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.guts.api.CommitIndex;
import com.github.jnthnclt.os.lab.core.guts.api.IndexFactory;
import com.github.jnthnclt.os.lab.core.guts.api.KeyToString;
import com.github.jnthnclt.os.lab.core.guts.api.MergerBuilder;
import com.github.jnthnclt.os.lab.core.guts.api.ReadIndex;
import com.github.jnthnclt.os.lab.core.guts.api.Scanner;
import com.github.jnthnclt.os.lab.core.guts.api.SplitterBuilder;
import com.github.jnthnclt.os.lab.core.guts.api.TombstonedVersion;
import com.github.jnthnclt.os.lab.log.LABLogger;
import com.github.jnthnclt.os.lab.log.LABLoggerFactory;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

public class RangeStripedCompactableIndexes {

    private static final LABLogger LOG = LABLoggerFactory.getLogger();

    private final AtomicLong largestStripeId = new AtomicLong();
    private final AtomicLong largestIndexId = new AtomicLong();
    private final byte[] labId;
    private final LABStats stats;
    private final LABFiles labFiles;
    private final ExecutorService destroy;
    private final File root;
    private final String primaryName;
    private final int entriesBetweenLeaps;
    private final Object copyIndexOnWrite = new Object();
    private volatile ConcurrentSkipListMap<byte[], FileBackMergableIndexes> indexes;
    private volatile Entry<byte[], FileBackMergableIndexes>[] indexesArray;
    private final long splitWhenKeysTotalExceedsNBytes;
    private final long splitWhenValuesTotalExceedsNBytes;
    private final long splitWhenValuesAndKeysTotalExceedsNBytes;
    private final Rawhide rawhide;
    private final LRUConcurrentBAHLinkedHash<Leaps> leapsCache;
    private final Semaphore appendSemaphore = new Semaphore(Short.MAX_VALUE, true);
    private final boolean fsyncFileRenames;
    private final LABHashIndexType hashIndexType;
    private final double hashIndexLoadFactor;
    private final TombstonedVersion tombstonedVersion;
    private final RangeStripeSet rangeStripeSet;

    public RangeStripedCompactableIndexes(byte[] labId,
                                          LABStats stats,
                                          LABFiles labFiles,
                                          ExecutorService destroy,
                                          File root,
                                          String primaryName,
                                          int entriesBetweenLeaps,
                                          long splitWhenKeysTotalExceedsNBytes,
                                          long splitWhenValuesTotalExceedsNBytes,
                                          long splitWhenValuesAndKeysTotalExceedsNBytes,
                                          Rawhide rawhide,
                                          LRUConcurrentBAHLinkedHash<Leaps> leapsCache,
                                          boolean fsyncFileRenames,
                                          LABHashIndexType hashIndexType,
                                          double hashIndexLoadFactor,
                                          TombstonedVersion tombstonedVersion) throws Exception {

        this.labId = labId;
        this.stats = stats;
        this.labFiles = labFiles;
        this.destroy = destroy;
        this.root = root;
        this.primaryName = primaryName;
        this.entriesBetweenLeaps = entriesBetweenLeaps;
        this.splitWhenKeysTotalExceedsNBytes = splitWhenKeysTotalExceedsNBytes;
        this.splitWhenValuesTotalExceedsNBytes = splitWhenValuesTotalExceedsNBytes;
        this.splitWhenValuesAndKeysTotalExceedsNBytes = splitWhenValuesAndKeysTotalExceedsNBytes;
        this.rawhide = rawhide;
        this.leapsCache = leapsCache;
        this.fsyncFileRenames = fsyncFileRenames;
        this.hashIndexType = hashIndexType;
        this.hashIndexLoadFactor = hashIndexLoadFactor;
        this.indexes = new ConcurrentSkipListMap<>(rawhide.getKeyComparator());
        this.tombstonedVersion = tombstonedVersion;

        this.rangeStripeSet = new RangeStripeSet(labId,
                stats,
                rawhide,
                leapsCache,
                labFiles);

        rangeStripeSet.load(new File(root, primaryName), largestIndexId, (stripeId, rangeStripe) -> {
            if (largestStripeId.get() < stripeId) {
                largestStripeId.set(stripeId);
            }

            indexes.put(rangeStripe.keyRange.start, new FileBackMergableIndexes(destroy,
                    largestStripeId,
                    largestIndexId,
                    root,
                    primaryName,
                    stripeId,
                    rangeStripe.mergeableIndexes));
        });

        indexesArray = indexes.entrySet().toArray(new Entry[0]);

    }

    @Override
    public String toString() {
        return "RangeStripedCompactableIndexes{"
                + "largestStripeId=" + largestStripeId
                + ", largestIndexId=" + largestIndexId
                + ", root=" + root
                + ", indexName=" + primaryName
                + ", entriesBetweenLeaps=" + entriesBetweenLeaps
                + ", splitWhenKeysTotalExceedsNBytes=" + splitWhenKeysTotalExceedsNBytes
                + ", splitWhenValuesTotalExceedsNBytes=" + splitWhenValuesTotalExceedsNBytes
                + ", splitWhenValuesAndKeysTotalExceedsNBytes=" + splitWhenValuesAndKeysTotalExceedsNBytes
                + '}';
    }

    private class FileBackMergableIndexes implements SplitterBuilder, MergerBuilder {

        final ExecutorService destroy;
        final AtomicLong largestStripeId;
        final AtomicLong largestIndexId;

        final File root;
        final String indexName;
        final long stripeId;
        final CompactableIndexes compactableIndexes;

        public FileBackMergableIndexes(ExecutorService destroy,
                                       AtomicLong largestStripeId,
                                       AtomicLong largestIndexId,
                                       File root,
                                       String indexName,
                                       long stripeId,
                                       CompactableIndexes mergeableIndexes) {

            this.destroy = destroy;
            this.largestStripeId = largestStripeId;
            this.largestIndexId = largestIndexId;
            this.compactableIndexes = mergeableIndexes;

            this.root = root;
            this.indexName = indexName;
            this.stripeId = stripeId;

            File indexRoot = new File(root, indexName);
            File stripeRoot = new File(indexRoot, String.valueOf(stripeId));

            File mergingRoot = new File(stripeRoot, "merging");
            File commitingRoot = new File(stripeRoot, "commiting");
            File splittingRoot = new File(stripeRoot, "splitting");

            FileUtils.deleteQuietly(mergingRoot);
            FileUtils.deleteQuietly(commitingRoot);
            FileUtils.deleteQuietly(splittingRoot);
        }

        void append(String rawhideName,
                    long appendVersion,
                    LABMemoryIndex memoryIndex,
                    byte[] minKey,
                    byte[] maxKey,
                    boolean fsync,
                    BolBuffer keyBuffer,
                    BolBuffer entryBuffer,
                    BolBuffer entryKeyBuffer) throws Exception {

            ReadOnlyIndex readOnlyIndex = flushMemoryIndexToDisk(
                    rawhideName,
                    memoryIndex,
                    minKey,
                    maxKey,
                    largestIndexId.incrementAndGet(),
                    0,
                    fsync,
                    keyBuffer,
                    entryBuffer,
                    entryKeyBuffer);

            if (readOnlyIndex != null && compactableIndexes.append(readOnlyIndex)) {
                if (labFiles != null) {
                    labFiles.add(labId, appendVersion, readOnlyIndex.getFile());
                }
            }
        }

        private ReadOnlyIndex flushMemoryIndexToDisk(
                String rawhideName,
                LABMemoryIndex memoryIndex,
                byte[] minKey,
                byte[] maxKey,
                long nextIndexId,
                int generation,
                boolean fsync,
                BolBuffer keyBuffer,
                BolBuffer entryBuffer,
                BolBuffer entryKeyBuffer) throws Exception {

            File indexRoot = new File(root, indexName);
            File stripeRoot = new File(indexRoot, String.valueOf(stripeId));
            File activeRoot = new File(stripeRoot, "active");
            File commitingRoot = new File(stripeRoot, "commiting");
            FileUtils.forceMkdir(commitingRoot);

            long count = memoryIndex.count();
            LOG.debug("Commiting memory index to on disk index: {}", count, activeRoot);

            int maxLeaps = calculateIdealMaxLeaps(count, entriesBetweenLeaps);
            IndexRangeId indexRangeId = new IndexRangeId(nextIndexId, nextIndexId, generation);
            File commitingIndexFile = indexRangeId.toFile(commitingRoot);
            FileUtils.deleteQuietly(commitingIndexFile);
            AppendOnlyFile appendOnlyFile = new AppendOnlyFile(commitingIndexFile);
            LABAppendableIndex appendableIndex = null;
            boolean exists = false;
            try {
                appendableIndex = new LABAppendableIndex(stats.bytesWrittenAsIndex,
                        indexRangeId,
                        appendOnlyFile,
                        maxLeaps,
                        entriesBetweenLeaps,
                        rawhide,
                        hashIndexType,
                        hashIndexLoadFactor,
                        tombstonedVersion);
                appendableIndex.append((stream) -> {

                    ReadIndex reader = memoryIndex.acquireReader();
                    try {
                        Scanner scanner = reader.rangeScan(false, false, minKey, maxKey, entryBuffer, entryKeyBuffer);
                        if (scanner != null) {
                            try {
                                BolBuffer rawEntry = new BolBuffer();
                                while ((rawEntry = scanner.next(rawEntry, null)) != null) {
                                    stream.stream(rawEntry);
                                }
                            } finally {
                                scanner.close();
                            }
                        }
                        return true;
                    } finally {
                        reader.release();
                    }
                }, keyBuffer);
                if (appendableIndex.getCount() > 0) {
                    appendableIndex.closeAppendable(fsync);
                    exists = true;
                } else {
                    appendableIndex.delete();
                }
            } catch (Exception x) {
                try {
                    if (appendableIndex != null) {
                        appendableIndex.close();
                    } else {
                        appendOnlyFile.close(); // sigh
                    }
                    appendOnlyFile.delete();
                } catch (Exception xx) {
                    LOG.error("Failed while trying to cleanup during a failure.", xx);
                }
                throw x;
            }

            File commitedIndexFile = indexRangeId.toFile(activeRoot);
            if (exists) { // Merge can cause index to disappear if all items are tombstoned and removed by ttl
                return moveIntoPlace(rawhideName, commitingIndexFile, commitedIndexFile, indexRangeId);
            } else {
                FileUtils.deleteDirectory(commitedIndexFile.getParentFile());
                FileUtils.deleteDirectory(commitingIndexFile.getParentFile());
                return null;
            }
        }

        private ReadOnlyIndex moveIntoPlace(String rawhideName,
                                            File commitingIndexFile,
                                            File commitedIndexFile,
                                            IndexRangeId indexRangeId) throws Exception {

            FileUtils.forceMkdir(commitedIndexFile.getParentFile());
            Files.move(commitingIndexFile.toPath(), commitedIndexFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
            ReadOnlyFile readOnlyFile = new ReadOnlyFile(commitedIndexFile);
            ReadOnlyIndex reopenedIndex = new ReadOnlyIndex(indexRangeId, readOnlyFile, rawhide, leapsCache);
            if (fsyncFileRenames) {
                reopenedIndex.fsync();  // Sorry
                // TODO Files.fsync index when java 9 supports it.
            }
            LOG.inc("movedIntoPlace");
            LOG.inc("movedIntoPlace>" + rawhideName);
            int histo = (int) Math.pow(2, UIO.chunkPower(readOnlyFile.length(), 0));
            LOG.inc("movedIntoPlace>" + histo);
            LOG.inc("movedIntoPlace>" + rawhideName + ">" + histo);

            return reopenedIndex;
        }

        boolean tx(int index,
                   boolean pointFrom,
                   byte[] fromKey,
                   byte[] toKey,
                   ReaderTx tx,
                   boolean hydrateValues) throws Exception {
            return compactableIndexes.tx(index, pointFrom, fromKey, toKey, tx, hydrateValues);
        }

        long count() throws Exception {
            return compactableIndexes.count();
        }

        void close() throws Exception {
            compactableIndexes.close();
        }

        int debt() {
            return compactableIndexes.debt();
        }

        Callable<Void> compactor(String rawhideName, int minMergeDebt, boolean fsync) throws Exception {
            return compactableIndexes.compactor(stats,
                    rawhideName,
                    splitWhenKeysTotalExceedsNBytes,
                    splitWhenValuesTotalExceedsNBytes,
                    splitWhenValuesAndKeysTotalExceedsNBytes,
                    this,
                    minMergeDebt,
                    fsync,
                    this);
        }

        @Override
        public Callable<Void> buildSplitter(String rawhideName,
                                            boolean fsync,
                                            SplitterBuilderCallback callback) throws Exception {

            File indexRoot = new File(root, indexName);
            File stripeRoot = new File(indexRoot, String.valueOf(stripeId));
            File mergingRoot = new File(stripeRoot, "merging");
            File commitingRoot = new File(stripeRoot, "commiting");
            File splittingRoot = new File(stripeRoot, "splitting");
            FileUtils.forceMkdir(mergingRoot);
            FileUtils.forceMkdir(commitingRoot);
            FileUtils.forceMkdir(splittingRoot);

            long nextStripeIdLeft = largestStripeId.incrementAndGet();
            long nextStripeIdRight = largestStripeId.incrementAndGet();
            FileBackMergableIndexes self = this;
            LOG.inc("split");
            return () -> {
                appendSemaphore.acquire(Short.MAX_VALUE);
                try {

                    IndexFactory leftHalfIndexFactory = (id, worstCaseCount) -> {
                        int maxLeaps = calculateIdealMaxLeaps(worstCaseCount, entriesBetweenLeaps);
                        File splitIntoDir = new File(splittingRoot, String.valueOf(nextStripeIdLeft));
                        FileUtils.deleteQuietly(splitIntoDir);
                        FileUtils.forceMkdir(splitIntoDir);
                        File splittingIndexFile = id.toFile(splitIntoDir);
                        LOG.debug("Creating new index for split: {}", splittingIndexFile);
                        AppendOnlyFile appendOnlyFile = new AppendOnlyFile(splittingIndexFile);
                        return new LABAppendableIndex(stats.bytesWrittenAsSplit,
                                id,
                                appendOnlyFile,
                                maxLeaps,
                                entriesBetweenLeaps,
                                rawhide,
                                hashIndexType,
                                hashIndexLoadFactor,
                                tombstonedVersion);
                    };

                    IndexFactory rightHalfIndexFactory = (id, worstCaseCount) -> {
                        int maxLeaps = calculateIdealMaxLeaps(worstCaseCount, entriesBetweenLeaps);
                        File splitIntoDir = new File(splittingRoot, String.valueOf(nextStripeIdRight));
                        FileUtils.deleteQuietly(splitIntoDir);
                        FileUtils.forceMkdir(splitIntoDir);
                        File splittingIndexFile = id.toFile(splitIntoDir);
                        LOG.debug("Creating new index for split: {}", splittingIndexFile);
                        AppendOnlyFile appendOnlyFile = new AppendOnlyFile(splittingIndexFile);
                        return new LABAppendableIndex(stats.bytesWrittenAsSplit,
                                id,
                                appendOnlyFile,
                                maxLeaps,
                                entriesBetweenLeaps,
                                rawhide,
                                hashIndexType,
                                hashIndexLoadFactor,
                                tombstonedVersion);
                    };

                    CommitIndex commitIndex = (ids) -> {
                        File left = new File(indexRoot, String.valueOf(nextStripeIdLeft));
                        File leftActive = new File(left, "active");
                        FileUtils.forceMkdir(leftActive.getParentFile());
                        File right = new File(indexRoot, String.valueOf(nextStripeIdRight));
                        File rightActive = new File(right, "active");
                        FileUtils.forceMkdir(rightActive.getParentFile());
                        LOG.debug("Commiting split:{} became left:{} right:{}", stripeRoot, left, right);

                        try {
                            Files.move(new File(splittingRoot, String.valueOf(nextStripeIdLeft)).toPath(),
                                    leftActive.toPath(),
                                    StandardCopyOption.ATOMIC_MOVE);
                            Files.move(new File(splittingRoot, String.valueOf(nextStripeIdRight)).toPath(),
                                    rightActive.toPath(),
                                    StandardCopyOption.ATOMIC_MOVE);

                            RangeStripe leftStripe = rangeStripeSet.loadStripe(left, largestIndexId);
                            RangeStripe rightStripe = rangeStripeSet.loadStripe(right, largestIndexId);
                            synchronized (copyIndexOnWrite) {
                                ConcurrentSkipListMap<byte[], FileBackMergableIndexes> copyOfIndexes = new ConcurrentSkipListMap<>(
                                        rawhide.getKeyComparator());
                                copyOfIndexes.putAll(indexes);

                                for (Iterator<Entry<byte[], FileBackMergableIndexes>> iterator = copyOfIndexes.entrySet()
                                        .iterator(); iterator.hasNext();
                                ) {
                                    Entry<byte[], FileBackMergableIndexes> next = iterator.next();
                                    if (next.getValue() == self) {
                                        iterator.remove();
                                        break;
                                    }
                                }
                                if (leftStripe != null && leftStripe.keyRange != null && leftStripe.keyRange.start != null) {
                                    copyOfIndexes.put(leftStripe.keyRange.start,
                                            new FileBackMergableIndexes(destroy, largestStripeId, largestIndexId, root,
                                                    indexName, nextStripeIdLeft,
                                                    leftStripe.mergeableIndexes));
                                }

                                if (rightStripe != null && rightStripe.keyRange != null && rightStripe.keyRange.start != null) {
                                    copyOfIndexes.put(rightStripe.keyRange.start,
                                            new FileBackMergableIndexes(destroy, largestStripeId, largestIndexId, root,
                                                    indexName, nextStripeIdRight,
                                                    rightStripe.mergeableIndexes));
                                }
                                indexes = copyOfIndexes;
                                indexesArray = copyOfIndexes.entrySet().toArray(new Entry[0]);
                            }
                            compactableIndexes.destroy(destroy, labFiles);
                            FileUtils.deleteQuietly(mergingRoot);
                            FileUtils.deleteQuietly(commitingRoot);
                            FileUtils.deleteQuietly(splittingRoot);

                            LOG.debug("Completed split:{} became left:{} right:{}", stripeRoot, left, right);
                            return null;
                        } catch (Exception x) {
                            FileUtils.deleteQuietly(left);
                            FileUtils.deleteQuietly(right);
                            LOG.error("Failed to split:{} became left:{} right:{}",
                                    new Object[]{stripeRoot, left, right}, x);
                            throw x;
                        }
                    };

                    Callable<Void> call = callback.call(leftHalfIndexFactory,
                            rightHalfIndexFactory,
                            commitIndex,
                            fsync,
                            destroy,
                            labFiles);

                    return call.call();

                } finally {
                    appendSemaphore.release(Short.MAX_VALUE);
                }
            };

        }

        @Override
        public Callable<Void> build(String rawhideName,
                                    int minimumRun,
                                    boolean fsync,
                                    MergerBuilderCallback callback) throws Exception {
            File indexRoot = new File(root, indexName);
            File stripeRoot = new File(indexRoot, String.valueOf(stripeId));
            File activeRoot = new File(stripeRoot, "active");
            File mergingRoot = new File(stripeRoot, "merging");
            FileUtils.forceMkdir(mergingRoot);

            IndexFactory indexFactory = (id, count) -> {
                int maxLeaps = calculateIdealMaxLeaps(count, entriesBetweenLeaps);
                File mergingIndexFile = id.toFile(mergingRoot);
                FileUtils.deleteQuietly(mergingIndexFile);
                AppendOnlyFile appendOnlyFile = new AppendOnlyFile(mergingIndexFile);
                return new LABAppendableIndex(stats.bytesWrittenAsMerge,
                        id,
                        appendOnlyFile,
                        maxLeaps,
                        entriesBetweenLeaps,
                        rawhide,
                        hashIndexType,
                        hashIndexLoadFactor,
                        tombstonedVersion);
            };

            CommitIndex commitIndex = (ids) -> {
                File mergedIndexFile = ids.get(0).toFile(mergingRoot);
                File file = ids.get(0).toFile(activeRoot);
                FileUtils.deleteQuietly(file);
                return moveIntoPlace(rawhideName, mergedIndexFile, file, ids.get(0));
            };

            return callback.call(minimumRun, fsync, indexFactory, commitIndex, destroy, labFiles);
        }

        private void auditRanges(String prefix, KeyToString keyToString) {
            compactableIndexes.auditRanges(prefix, keyToString);
        }

    }

    private static class Stripe {

        final KeyRange keyRange;
        final CompactableIndexes mergeableIndexes;

        public Stripe(KeyRange keyRange, CompactableIndexes mergeableIndexes) {
            this.keyRange = keyRange;
            this.mergeableIndexes = mergeableIndexes;
        }

    }

    public void append(String rawhideName,
                       long appendVersion,
                       LABMemoryIndex memoryIndex,
                       boolean fsync,
                       BolBuffer keyBuffer,
                       BolBuffer entryBuffer,
                       BolBuffer entryKeyBuffer) throws Exception {

        appendSemaphore.acquire();
        try {
            BolBuffer minKey = new BolBuffer(memoryIndex.minKey());
            BolBuffer maxKey = new BolBuffer(memoryIndex.maxKey());

            if (indexes.isEmpty()) {
                long stripeId = largestStripeId.incrementAndGet();
                FileBackMergableIndexes index = new FileBackMergableIndexes(destroy,
                        largestStripeId,
                        largestIndexId,
                        root,
                        primaryName,
                        stripeId,
                        new CompactableIndexes(stats, rawhide));

                index.append(rawhideName, appendVersion, memoryIndex, null, null, fsync, keyBuffer, entryBuffer,
                        entryKeyBuffer);

                synchronized (copyIndexOnWrite) {
                    ConcurrentSkipListMap<byte[], FileBackMergableIndexes> copyOfIndexes = new ConcurrentSkipListMap<>(
                            rawhide.getKeyComparator());
                    copyOfIndexes.putAll(indexes);
                    copyOfIndexes.put(minKey.bytes, index);
                    indexes = copyOfIndexes;
                    indexesArray = copyOfIndexes.entrySet().toArray(new Entry[0]);
                }
                return;
            }

            SortedMap<byte[], FileBackMergableIndexes> tailMap = indexes.tailMap(minKey.bytes);
            if (tailMap.isEmpty()) {
                tailMap = indexes.tailMap(indexes.lastKey());
            } else {
                byte[] priorKey = indexes.lowerKey(tailMap.firstKey());
                if (priorKey == null) {
                    FileBackMergableIndexes moved;
                    synchronized (copyIndexOnWrite) {
                        ConcurrentSkipListMap<byte[], FileBackMergableIndexes> copyOfIndexes = new ConcurrentSkipListMap<>(
                                rawhide.getKeyComparator());
                        copyOfIndexes.putAll(indexes);
                        moved = copyOfIndexes.remove(tailMap.firstKey());
                        copyOfIndexes.put(minKey.bytes, moved);
                        indexes = copyOfIndexes;
                        indexesArray = copyOfIndexes.entrySet().toArray(new Entry[0]);
                    }
                    tailMap = indexes.tailMap(minKey.bytes);
                } else {
                    tailMap = indexes.tailMap(priorKey);
                }
            }

            Comparator<byte[]> keyComparator = rawhide.getKeyComparator();
            Map.Entry<byte[], FileBackMergableIndexes> priorEntry = null;
            for (Map.Entry<byte[], FileBackMergableIndexes> currentEntry : tailMap.entrySet()) {
                if (priorEntry == null) {
                    priorEntry = currentEntry;
                } else {
                    if (memoryIndex.containsKeyInRange(priorEntry.getKey(), currentEntry.getKey())) {
                        priorEntry.getValue().append(rawhideName,
                                appendVersion,
                                memoryIndex,
                                priorEntry.getKey(),
                                currentEntry.getKey(),
                                fsync,
                                keyBuffer,
                                entryBuffer,
                                entryKeyBuffer);
                    }
                    priorEntry = currentEntry;
                    if (keyComparator.compare(maxKey.bytes, currentEntry.getKey()) < 0) {
                        priorEntry = null;
                        break;
                    }
                }
            }
            if (priorEntry != null && memoryIndex.containsKeyInRange(priorEntry.getKey(), null)) {
                priorEntry.getValue().append(rawhideName,
                        appendVersion,
                        memoryIndex,
                        priorEntry.getKey(),
                        null,
                        fsync,
                        keyBuffer,
                        entryBuffer,
                        entryKeyBuffer);
            }

        } finally {
            appendSemaphore.release();
        }

    }

    public boolean pointTx(Keys keys,
                           long newerThanTimestamp,
                           long newerThanTimestampVersion,
                           ReadIndex reader,
                           ReadIndex flushingReader,
                           boolean hashIndexEnabled,
                           boolean hydrateValues,
                           BolBuffer streamKeyBuffer,
                           BolBuffer streamValueBuffer,
                           ValueStream valueStream) throws Exception {

        ReaderTx tx = (index, pointFrom, fromKey, toKey, acquired, hydrateValues1) -> {
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
            }
            System.arraycopy(acquired, 0, indexes, active + flushing, acquired.length);
            BolBuffer next = PointInterleave.get(indexes, fromKey, rawhide, hashIndexEnabled);
            return rawhide.streamRawEntry(index, next, streamKeyBuffer, streamValueBuffer, valueStream);
        };

        return keys.keys((index, key, offset, length) -> {
            rangeTx(index, true, key, key, newerThanTimestamp, newerThanTimestampVersion, tx, hydrateValues);
            return true;
        });
    }

    private static final ReadIndex[] EMPTY = new ReadIndex[0];

    public boolean rangeTx(int index,
                           boolean pointFrom,
                           byte[] from,
                           byte[] to,
                           long newerThanTimestamp,
                           long newerThanTimestampVersion,
                           ReaderTx tx,
                           boolean hydrateValues) throws Exception {

        Comparator<byte[]> comparator = rawhide.getKeyComparator();

        THE_INSANITY:
        while (true) {

            Entry<byte[], FileBackMergableIndexes>[] entries = indexesArray;
            if (entries == null || entries.length == 0) {
                return tx.tx(index, pointFrom, from, to, EMPTY, hydrateValues);
            }

            int fi = 0;
            if (from != null) {
                int i = binarySearch(comparator, entries, from, 0);
                if (i > -1) {
                    fi = i;
                } else {
                    int insertion = (-i) - 1;
                    fi = insertion == 0 ? 0 : insertion - 1;
                }
            }


            int ti = entries.length - 1;
            if (to != null) {
                int i = binarySearch(comparator, entries, to, fi);
                if (i > -1) {
                    ti = from != null && Arrays.equals(from, to) ? i : i - 1;
                } else {
                    int insertion = (-i) - 1;
                    if (insertion == 0) {
                        return tx.tx(index, pointFrom, from, to, EMPTY, hydrateValues);
                    }
                    ti = insertion - 1;
                }
            }

            boolean streamed = false;
            for (int i = fi; i <= ti; i++) {
                Entry<byte[], FileBackMergableIndexes> entry = entries[i];
                byte[] start = i == fi ? from : entries[i].getKey();
                byte[] end = i < ti ? entries[i + 1].getKey() : to;
                FileBackMergableIndexes mergableIndex = entry.getValue();
                try {
                    TimestampAndVersion timestampAndVersion = mergableIndex.compactableIndexes.maxTimeStampAndVersion();
                    if (rawhide.mightContain(timestampAndVersion.maxTimestamp,
                            timestampAndVersion.maxTimestampVersion,
                            newerThanTimestamp,
                            newerThanTimestampVersion)) {
                        streamed = true;
                        if (!mergableIndex.tx(index, pointFrom, start, end, tx, hydrateValues)) {
                            return false;
                        }
                    }
                } catch (LABConcurrentSplitException cse) {
                    from = (i == fi) ? from : entry.getKey(); // Sorry! Dont rewind from where from is haha
                    continue THE_INSANITY;
                }
            }
            if (!streamed) {
                return tx.tx(index, pointFrom, from, to, EMPTY, hydrateValues);
            }
            return true;
        }

    }


    private static int binarySearch(
            Comparator<byte[]> comparator,
            Entry<byte[], ?>[] a,
            byte[] key,
            int low) {

        int high = a.length - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            Entry<byte[], ?> midVal = a[mid];

            int c = comparator.compare(midVal.getKey(), key);
            if (c < 0) {
                low = mid + 1;
            } else if (c > 0) {
                high = mid - 1;
            } else {
                return mid; // key found
            }
        }
        return -(low + 1);  // key not found.
    }

    public int debt() {
        int maxDebt = 0;
        for (FileBackMergableIndexes index : indexes.values()) {
            maxDebt = Math.max(index.debt(), maxDebt);
        }
        return maxDebt;
    }

    public List<Callable<Void>> buildCompactors(String rawhideName, boolean fsync, int minDebt) throws Exception {
        List<Callable<Void>> compactors = null;
        for (FileBackMergableIndexes index : indexes.values()) {
            if (index.debt() >= minDebt) {
                Callable<Void> compactor = index.compactor(rawhideName, minDebt, fsync);
                if (compactor != null) {
                    if (compactors == null) {
                        compactors = new ArrayList<>();
                    }
                    compactors.add(compactor);
                }
            }
        }
        return compactors;
    }

    public boolean isEmpty() {
        return indexes.isEmpty();
    }

    public long count() throws Exception {
        long count = 0;
        for (FileBackMergableIndexes index : indexes.values()) {
            count += index.count();
        }
        return count;
    }

    public void close() throws Exception {
        for (FileBackMergableIndexes index : indexes.values()) {
            index.close();
        }
    }

    public static int calculateIdealMaxLeaps(long entryCount, int entriesBetweenLeaps) {
        int approximateLeapCount = (int) Math.max(1, entryCount / entriesBetweenLeaps);
        int maxLeaps = (int) (Math.log(approximateLeapCount) / Math.log(2));
        return 1 + maxLeaps;
    }

    public void auditRanges(KeyToString keyToString) {
        for (Entry<byte[], FileBackMergableIndexes> entry : indexes.entrySet()) {

            System.out.println("key:" + keyToString.keyToString(entry.getKey()));
            FileBackMergableIndexes indexes = entry.getValue();
            indexes.auditRanges("\t\t", keyToString);
        }
    }

}