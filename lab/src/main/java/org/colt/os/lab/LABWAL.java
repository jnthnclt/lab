package org.colt.os.lab;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.collections.bah.BAHEqualer;
import com.jivesoftware.os.jive.utils.collections.bah.BAHMapState;
import com.jivesoftware.os.jive.utils.collections.bah.BAHash;
import com.jivesoftware.os.jive.utils.collections.bah.BAHasher;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.colt.os.lab.api.FormatTransformer;
import org.colt.os.lab.api.FormatTransformerProvider;
import org.colt.os.lab.api.JournalStream;
import org.colt.os.lab.api.RawEntryFormat;
import org.colt.os.lab.api.ValueIndex;
import org.colt.os.lab.api.ValueIndexConfig;
import org.colt.os.lab.api.ValueStream;
import org.colt.os.lab.api.exceptions.LABClosedException;
import org.colt.os.lab.api.exceptions.LABCorruptedException;
import org.colt.os.lab.api.exceptions.LABFailedToInitializeWALException;
import org.colt.os.lab.api.rawhide.Rawhide;
import org.colt.os.lab.guts.AppendOnlyFile;
import org.colt.os.lab.guts.ReadOnlyFile;
import org.colt.os.lab.io.AppendableHeap;
import org.colt.os.lab.io.BolBuffer;
import org.colt.os.lab.io.PointerReadableByteBufferFile;
import org.colt.os.lab.io.api.IAppendOnly;
import org.colt.os.lab.util.LABLogger;
import org.colt.os.lab.util.LABLoggerFactory;

/**
 * @author jonathan.colt
 */
public class LABWAL {

    private static final LABLogger LOG = LABLoggerFactory.getLogger();;

    private static final byte ENTRY = 0;
    private static final byte BATCH_ISOLATION = 1;
    private static final byte COMMIT_ISOLATION = 2;
    private static final int[] MAGIC = new int[3];

    static {
        MAGIC[ENTRY] = 351126232;
        MAGIC[BATCH_ISOLATION] = 759984878;
        MAGIC[COMMIT_ISOLATION] = 266850631;
    }

    private final List<ActiveWAL> oldWALs = Lists.newCopyOnWriteArrayList();
    private final AtomicReference<ActiveWAL> activeWAL = new AtomicReference<>();
    private final AtomicLong walIdProvider = new AtomicLong();
    private final Semaphore semaphore = new Semaphore(Short.MAX_VALUE, true);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final LABStats stats;
    private final File walRoot;
    private final long maxWALSizeInBytes;
    private final long maxEntriesPerWAL;
    private final long maxEntrySizeInBytes;
    private final long maxValueIndexHeapPressureOverride;

    public LABWAL(LABStats stats,
        File walRoot,
        long maxWALSizeInBytes,
        long maxEntriesPerWAL,
        long maxEntrySizeInBytes,
        long maxValueIndexHeapPressureOverride) throws IOException {

        this.stats = stats;
        this.walRoot = walRoot;
        this.maxWALSizeInBytes = maxWALSizeInBytes;
        this.maxEntriesPerWAL = maxEntriesPerWAL;
        this.maxEntrySizeInBytes = maxEntrySizeInBytes;
        this.maxValueIndexHeapPressureOverride = maxValueIndexHeapPressureOverride;
    }

    public int oldWALCount() {
        return oldWALs.size();
    }

    public long activeWALId() {
        return walIdProvider.get();
    }

    public void open(LABEnvironment environment, JournalStream journalStream) throws Exception {

        File[] walFiles = walRoot.listFiles();
        if (walFiles == null) {
            return;
        }

        long maxWALId = 0;
        List<File> listWALFiles = Lists.newArrayList();
        for (File walFile : walFiles) {
            try {
                maxWALId = Math.max(maxWALId, Long.parseLong(walFile.getName()));
                listWALFiles.add(walFile);
            } catch (NumberFormatException nfe) {
                LOG.error("Encoudered an unexpected file name:" + walFile + " in " + walRoot);
            }
        }
        walIdProvider.set(maxWALId);

        Collections.sort(listWALFiles, (wal1, wal2) -> Long.compare(Long.parseLong(wal1.getName()), Long.parseLong(wal2.getName())));

        List<ReadOnlyFile> deleteableIndexFiles = Lists.newArrayList();
        Map<String, ListMultimap<Long, byte[]>> allEntries = Maps.newHashMap();

        Semaphore valueIndexesSemaphore = new Semaphore(Short.MAX_VALUE, true);
        BAHash<ValueIndex> valueIndexes = new BAHash<>(new BAHMapState<>(10, true, BAHMapState.NIL), BAHasher.SINGLETON, BAHEqualer.SINGLETON);
        BolBuffer rawEntryBuffer = new BolBuffer();
        BolBuffer keyBuffer = new BolBuffer();
        for (File walFile : listWALFiles) {

            ReadOnlyFile readOnlyFile = null;
            try {
                readOnlyFile = new ReadOnlyFile(walFile);
                deleteableIndexFiles.add(readOnlyFile);

                PointerReadableByteBufferFile reader = readOnlyFile.pointerReadable(-1);

                long offset = 0;
                try {
                    long[] appended = new long[1];
                    while (true) {
                        int rowType = reader.read(offset);
                        offset++;
                        if (rowType == -1) {
                            break; //EOF
                        }
                        if (rowType > 3) {
                            throw new LABCorruptedException("expected a row type greater than -1 and less than 128 but encountered " + rowType);
                        }
                        int magic = reader.readInt(offset);
                        offset += 4;
                        if (magic != MAGIC[rowType]) {
                            throw new LABCorruptedException("expected a magic " + MAGIC[rowType] + " but encountered " + magic);
                        }
                        int valueIndexIdLength = reader.readInt(offset);
                        offset += 4;
                        if (valueIndexIdLength >= maxEntrySizeInBytes) {
                            throw new LABCorruptedException("valueIndexId length corruption" + valueIndexIdLength + ">=" + maxEntrySizeInBytes);
                        }

                        byte[] valueIndexId = new byte[valueIndexIdLength];
                        reader.read(offset, valueIndexId, 0, valueIndexIdLength);
                        offset += valueIndexIdLength;

                        String valueIndexKey = new String(valueIndexId, StandardCharsets.UTF_8);
                        long appendVersion = reader.readLong(offset);
                        offset += 8;

                        if (rowType == ENTRY) {
                            int entryLength = reader.readInt(offset);
                            offset += 4;
                            if (entryLength >= maxEntrySizeInBytes) {
                                throw new LABCorruptedException("entryLength length corruption" + entryLength + ">=" + maxEntrySizeInBytes);
                            }

                            byte[] entry = new byte[entryLength];
                            reader.read(offset, entry, 0, entryLength);
                            offset += entryLength;

                            ListMultimap<Long, byte[]> valueIndexVersionedEntries = allEntries.computeIfAbsent(valueIndexKey,
                                (k) -> ArrayListMultimap.create());
                            valueIndexVersionedEntries.put(appendVersion, entry);

                        } else if (rowType == BATCH_ISOLATION) {
                            ListMultimap<Long, byte[]> valueIndexVersionedEntries = allEntries.get(valueIndexKey);
                            if (valueIndexVersionedEntries != null) {
                                ValueIndexConfig valueIndexConfig = environment.valueIndexConfig(valueIndexKey.getBytes(StandardCharsets.UTF_8));
                                FormatTransformerProvider formatTransformerProvider = environment.formatTransformerProvider(
                                    valueIndexConfig.formatTransformerProviderName);
                                Rawhide rawhide = environment.rawhide(valueIndexConfig.rawhideName);
                                RawEntryFormat rawEntryFormat = environment.rawEntryFormat(valueIndexConfig.rawEntryFormatName);
                                LAB appendToValueIndex = (LAB) openValueIndex(environment, valueIndexId, valueIndexes);

                                FormatTransformer readKey = formatTransformerProvider.read(rawEntryFormat.getKeyFormat());
                                FormatTransformer readValue = formatTransformerProvider.read(rawEntryFormat.getValueFormat());

                                BolBuffer kb = new BolBuffer();
                                BolBuffer vb = new BolBuffer();
                                appendToValueIndex.onOpenAppend((stream) -> {

                                    ValueStream valueStream = (index, key, timestamp, tombstoned, version, payload) -> {
                                        byte[] keyBytes = key == null ? null : key.copy();
                                        byte[] payloadBytes = payload == null ? null : payload.copy();
                                        boolean result = stream.stream(index, keyBytes, timestamp, tombstoned, version,
                                            payloadBytes);
                                        if (journalStream != null) {
                                            journalStream.stream(valueIndexId, keyBytes, timestamp, tombstoned, version, payloadBytes);
                                        }
                                        appended[0]++;
                                        return result;
                                    };

                                    for (byte[] entry : valueIndexVersionedEntries.get(appendVersion)) {

                                        if (!rawhide.streamRawEntry(-1, readKey, readValue, new BolBuffer(entry), kb, vb, valueStream)) {
                                            return false;
                                        }
                                    }
                                    return true;
                                }, true, maxValueIndexHeapPressureOverride, rawEntryBuffer, keyBuffer);
                                valueIndexVersionedEntries.removeAll(appendVersion);
                            }
                        }
                    }
                    LOG.info("Appended {}", appended[0]);
                } catch (LABCorruptedException | EOFException x) {
                    LOG.warn("Corruption detected at fp:{} length:{} for file:{} cause:{}", offset, reader.length(), walFile, x.getClass());
                } catch (Exception x) {
                    LOG.error("Encountered an issue that requires intervention at fp:{} length:{} for file:{}",
                        new Object[] { offset, reader.length(), walFile }, x);
                    throw new LABFailedToInitializeWALException("Encountered an issue in " + walFile + " please help.", x);
                }
            } finally {
                if (readOnlyFile != null) {
                    readOnlyFile.close();
                }
            }
        }

        try {
            valueIndexes.stream(valueIndexesSemaphore, (byte[] key, ValueIndex value) -> {
                value.close(true, true);
                return true;
            });
        } catch (Exception x) {
            throw new LABFailedToInitializeWALException("Encountered an issue while commiting and closing. Please help.", x);
        }

        for (ReadOnlyFile deletableIndexFile : deleteableIndexFiles) {
            try {
                deletableIndexFile.delete();
                LOG.info("Cleanup WAL {}", deletableIndexFile);
            } catch (Exception x) {
                throw new LABFailedToInitializeWALException(
                    "Encountered an issue while deleting WAL:" + deletableIndexFile.getFileName() + ". Please help.", x);
            }
        }
    }

    private ValueIndex openValueIndex(LABEnvironment environment, byte[] valueIndexId, BAHash<ValueIndex> valueIndexes) throws Exception {
        ValueIndex valueIndex = valueIndexes.get(valueIndexId, 0, valueIndexId.length);
        if (valueIndex == null) {
            ValueIndexConfig valueIndexConfig = environment.valueIndexConfig(valueIndexId);
            valueIndex = environment.open(valueIndexConfig);
            valueIndexes.put(valueIndexId, valueIndex);
        }
        return valueIndex;
    }

    public void close(LABEnvironment environment) throws IOException, InterruptedException {
        semaphore.acquire(Short.MAX_VALUE);
        try {
            if (closed.compareAndSet(false, true)) {
                ActiveWAL wal = activeWAL.get();
                if (wal != null) {
                    wal.close();
                    activeWAL.set(null);
                }
                for (ActiveWAL oldWAL : oldWALs) {
                    oldWAL.close();
                }
                oldWALs.clear();
            }
        } finally {
            semaphore.release(Short.MAX_VALUE);
        }
    }

    public interface LabWALAppendTx {
        void append(ActiveWAL activeWAL) throws Exception;
    }

    public void appendTx(byte[] valueIndexId, long appendVersion, boolean fsync, LabWALAppendTx tx) throws Exception {
        boolean needToAllocateNewWAL = false;
        semaphore.acquire();
        try {
            if (closed.get()) {
                throw new LABClosedException("Trying to write to a Lab WAL that has been closed.");
            }
            ActiveWAL wal = activeWAL();
            tx.append(wal);
            wal.flushed(valueIndexId, appendVersion, fsync);
            if (wal.entryCount.get() > maxEntriesPerWAL || wal.sizeInBytes.get() > maxWALSizeInBytes) {
                needToAllocateNewWAL = true;
            }
        } finally {
            semaphore.release();
        }

        if (needToAllocateNewWAL) {
            semaphore.acquire(Short.MAX_VALUE);
            try {
                if (closed.get()) {
                    throw new LABClosedException("Trying to write to a Lab WAL that has been closed.");
                }
                ActiveWAL wal = activeWAL();
                if (wal.entryCount.get() > maxEntriesPerWAL || wal.sizeInBytes.get() > maxWALSizeInBytes) {
                    ActiveWAL oldWAL = activeWAL.getAndSet(allocateNewWAL());
                    oldWAL.close();
                    oldWALs.add(oldWAL);
                }
            } finally {
                semaphore.release(Short.MAX_VALUE);
            }
        }
    }

    public void commit(byte[] valueIndexId, long appendVersion, boolean fsync) throws Exception {
        List<ActiveWAL> removeable = null;
        semaphore.acquire(); // Keep somebody else from closing the WAL
        try {
            if (closed.get()) {
                throw new LABClosedException("Trying to write to a Lab WAL that has been closed.");
            }
            if (!oldWALs.isEmpty()) {
                removeable = Lists.newArrayList();
                for (ActiveWAL oldWAL : oldWALs) {
                    if (oldWAL.commit(valueIndexId, appendVersion)) {
                        removeable.add(oldWAL);
                    }
                }
            }
        } finally {
            semaphore.release();
        }

        if (removeable != null && !removeable.isEmpty()) {
            semaphore.acquire(Short.MAX_VALUE);
            try {
                if (closed.get()) {
                    throw new LABClosedException("Trying to write to a Lab WAL that has been closed.");
                }
                for (ActiveWAL remove : removeable) {
                    remove.delete();
                    LOG.info("Post commit WAL cleanup. {}", remove);
                }
                oldWALs.removeAll(removeable);
            } finally {
                semaphore.release(Short.MAX_VALUE);
            }
        }
    }

    private ActiveWAL activeWAL() throws Exception {
        ActiveWAL wal = activeWAL.get();
        if (wal == null) {
            synchronized (activeWAL) {
                wal = activeWAL.get();
                if (wal == null) {
                    wal = allocateNewWAL();
                    activeWAL.set(wal);
                }
            }
        }
        return wal;
    }

    private ActiveWAL allocateNewWAL() throws Exception {
        ActiveWAL wal;
        File file = new File(walRoot, String.valueOf(walIdProvider.incrementAndGet()));
        file.getParentFile().mkdirs();
        LOG.info("allocating new active wal {}", file);
        AppendOnlyFile appendOnlyFile = new AppendOnlyFile(file);
        wal = new ActiveWAL(stats, appendOnlyFile);
        return wal;
    }

    public static final class ActiveWAL {

        private final LABStats stats;
        private final AppendOnlyFile wal;
        private final IAppendOnly appendOnly;
        private final BAHash<Long> appendVersions;
        private final AtomicLong entryCount = new AtomicLong();
        private final AtomicLong sizeInBytes = new AtomicLong();
        private final Object oneWriteAtTimeLock = new Object();
        private final AppendableHeap appendableHeap = new AppendableHeap(8192);


        private ActiveWAL(LABStats stats, AppendOnlyFile wal) throws Exception {
            this.stats = stats;
            this.wal = wal;
            this.appendVersions = new BAHash<>(new BAHMapState<>(10, true, BAHMapState.NIL), BAHasher.SINGLETON, BAHEqualer.SINGLETON);
            this.appendOnly = wal.appender();
        }

        public void append(byte[] valueIndexId, long appendVersion, BolBuffer entry) throws Exception {
            entryCount.incrementAndGet();
            int sizeOfAppend = 1 + 4 + 4 + valueIndexId.length + 8 + entry.length;
            sizeInBytes.addAndGet(sizeOfAppend);
            synchronized (oneWriteAtTimeLock) {
                append(ENTRY, valueIndexId, appendVersion);
                appendableHeap.appendInt(entry.length);
                appendableHeap.append(entry.bytes, entry.offset, entry.length);
            }
            stats.bytesWrittenToWAL.add(sizeOfAppend);
        }

        private void flushed(byte[] valueIndexId, long appendVersion, boolean fsync) throws Exception {
            int numBytes = 1 + 4 + 4 + valueIndexId.length + 8;
            sizeInBytes.addAndGet(numBytes);
            synchronized (oneWriteAtTimeLock) {
                append(BATCH_ISOLATION, valueIndexId, appendVersion);

                appendOnly.append(appendableHeap.leakBytes(), 0, (int) appendableHeap.length());
                appendOnly.flush(fsync);

                appendableHeap.reset();
                appendVersions.put(valueIndexId, appendVersion);
            }
            stats.bytesWrittenToWAL.add(numBytes);
        }

        private void append(byte type, byte[] valueIndexId, long appendVersion) throws IOException {
            appendableHeap.appendByte(type);
            appendableHeap.appendInt(MAGIC[type]);
            appendableHeap.appendInt(valueIndexId.length);
            appendableHeap.append(valueIndexId, 0, valueIndexId.length);
            appendableHeap.appendLong(appendVersion);
        }

        private boolean commit(byte[] valueIndexId, long appendVersion) throws Exception {
            synchronized (oneWriteAtTimeLock) {
                Long lastAppendVersion = appendVersions.get(valueIndexId, 0, valueIndexId.length);
                if (lastAppendVersion != null && lastAppendVersion < appendVersion) {
                    appendVersions.remove(valueIndexId, 0, valueIndexId.length);
                }
                return appendVersions.size() == 0;
            }
        }

        private void close() throws IOException {
            wal.close();
        }

        private void delete() throws IOException {
            wal.close();
            wal.delete();
        }

        @Override
        public String toString() {
            return "ActiveWAL{" +
                "wal=" + wal.getFile() +
                ", entryCount=" + entryCount +
                ", sizeInBytes=" + sizeInBytes +
                '}';
        }
    }
}
