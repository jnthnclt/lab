package com.github.jnthnclt.os.lab.core.search;

import com.github.jnthnclt.os.lab.collections.bah.BAHash;
import com.github.jnthnclt.os.lab.core.LABIndexProvider;
import com.github.jnthnclt.os.lab.core.api.ValueIndex;
import com.github.jnthnclt.os.lab.core.bitmaps.LABBitmapIndex;
import com.github.jnthnclt.os.lab.core.bitmaps.LABBitmapIndexes;
import com.github.jnthnclt.os.lab.core.bitmaps.LABBitmaps;
import com.github.jnthnclt.os.lab.core.bitmaps.LABIndexKeyInterner;
import com.github.jnthnclt.os.lab.core.bitmaps.LABIndexKeyRange;
import com.github.jnthnclt.os.lab.core.bitmaps.LABStripingLocksProvider;
import com.github.jnthnclt.os.lab.core.bitmaps.RoaringLABBitmaps;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import com.github.jnthnclt.os.lab.core.io.api.UIO;
import com.github.jnthnclt.os.lab.core.util.LABLogger;
import com.github.jnthnclt.os.lab.core.util.LABLoggerFactory;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.commons.io.FileUtils;
import org.roaringbitmap.RoaringBitmap;

public class LABSearchIndex {

    private static final LABLogger LOG = LABLoggerFactory.getLogger();


    private final int cores = Runtime.getRuntime().availableProcessors();
    private final ExecutorService indexerThreads = Executors.newFixedThreadPool(cores);

    private final LABIndexProvider valueIndexProvider;
    private final File root;
    private final ValueIndex<byte[]> guidToIdx;
    private final ValueIndex<byte[]> fieldNameToFieldId;
    private final Map<String, ValueIndex<byte[]>> fieldNameBlob = Maps.newConcurrentMap();

    public final LABBitmapIndexes<RoaringBitmap, RoaringBitmap> fieldIndex;

    public LABSearchIndex(LABIndexProvider valueIndexProvider, File root) throws Exception {

        this.valueIndexProvider = valueIndexProvider;
        this.root = root;

        this.fieldNameToFieldId = valueIndexProvider.buildIndex(root, "fieldNameToFieldId");

        this.guidToIdx = valueIndexProvider.buildIndex(root, "guidToIndex");

        LABBitmaps<RoaringBitmap, RoaringBitmap> bitmaps = new RoaringLABBitmaps();

        ValueIndex<byte[]> fieldBitmapIndex = valueIndexProvider.buildIndex(root, "fieldBitmaps");
        ValueIndex<byte[]> fieldTermIndex = valueIndexProvider.buildIndex(root, "fieldTerms");
        ValueIndex<byte[]> fieldCardinalitiesIndex = valueIndexProvider.buildIndex(root, "fieldCardinalities");

        AtomicLong version = new AtomicLong();
        fieldIndex = new LABBitmapIndexes<>(
            () -> version.getAndIncrement(),
            bitmaps,
            new byte[] { 0 },
            (ValueIndex<byte[]>[]) new ValueIndex[] { fieldBitmapIndex },
            new byte[] { 1 },
            (ValueIndex<byte[]>[]) new ValueIndex[] { fieldTermIndex },
            new byte[] { 2 },
            (ValueIndex<byte[]>[]) new ValueIndex[] { fieldCardinalitiesIndex },
            new LABStripingLocksProvider(1024),
            new LABIndexKeyInterner(true));

        if (root != null) {
            LOG.info(root.getAbsolutePath());
            File[] files = root.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        String name = file.getName();
                        if (name.startsWith("fieldName_")) {
                            getOrCreateFieldNameBlob(name);
                        }
                    }
                }
            }
        }
    }

    public List<String> fields() throws Exception {
        List<String> fields = Lists.newArrayList();
        fieldNameToFieldId.rowScan((i, key, l, b, l1, value) -> {
            if (key != null) {
                fields.add(new String(key.copy(), StandardCharsets.UTF_8));
            }
            return true;
        }, true);
        return fields;
    }

    private final Cache<String, Integer> cache = CacheBuilder.newBuilder().maximumSize(10_000).build();

    public int fieldOrdinal(String fieldName) throws Exception {
        if (fieldName == null || fieldName.length() == 0) {
            return -1;
        }
        Integer got = cache.getIfPresent(fieldName);
        if (got == null) {
            got = cache.get(fieldName, () -> lut(fieldNameToFieldId, fieldName.getBytes(StandardCharsets.UTF_8)));
        }
        if (got == null) {
            return -1;
        }
        return got;
    }
    public void fieldIntegerValues(int fieldOrdinal, List<LABIndexKeyRange> ranges,Function<Integer, Boolean> fieldValues) throws Exception {
        fieldIndex.streamTermIdsForField(fieldOrdinal, ranges, labIndexKey -> {
            if (labIndexKey != null && labIndexKey.length() == 4) {
                if (!fieldValues.apply(UIO.bytesInt(labIndexKey.getBytes()))) {
                    return false;
                }
            }
            return true;
        });
    }

    public void fieldLongValues(int fieldOrdinal, List<LABIndexKeyRange> ranges, Function<Long, Boolean> fieldValues) throws Exception {
        fieldIndex.streamTermIdsForField(fieldOrdinal, ranges, labIndexKey -> {
            if (labIndexKey != null && labIndexKey.length() == 8) {
                if (!fieldValues.apply(UIO.bytesLong(labIndexKey.getBytes()))) {
                    return false;
                }
            }
            return true;
        });
    }

    public void fieldStringValues(int fieldOrdinal, List<LABIndexKeyRange> ranges,Function<String, Boolean> fieldValues) throws Exception {
        fieldIndex.streamTermIdsForField(fieldOrdinal, ranges, labIndexKey -> {
            if (labIndexKey != null && labIndexKey.length() > 0) {
                String s = new String(labIndexKey.getBytes(), StandardCharsets.UTF_8);
                if (s.length() > 0) {
                    if (!fieldValues.apply(s)) {
                        return false;
                    }
                }
            }
            return true;
        });
    }


    public RoaringBitmap bitmap(int fieldOrdinal, String key) throws Exception {
        return bitmap(fieldOrdinal, key);
    }

    public RoaringBitmap bitmap(int fieldOrdinal, int key) throws Exception {
        return bitmap(fieldOrdinal, UIO.intBytes(key, new byte[4], 0));
    }

    public RoaringBitmap bitmap(int fieldOrdinal, long key) throws Exception {
        return bitmap(fieldOrdinal, UIO.longBytes(key, new byte[8], 0));
    }

    public RoaringBitmap bitmap(int fieldOrdinal, byte[] key) throws Exception {
        LABBitmapIndex<RoaringBitmap, RoaringBitmap> got = fieldIndex.get(fieldOrdinal, key);
        return got == null ? null : got.txIndex(bitmap -> bitmap);
    }


    public interface StreamValues {
        boolean value(int index, BolBuffer value) throws Exception;
    }

    public void storedValues(Collection<Long> externalIds, int fieldNameOrdinal, StreamValues values) throws Exception {
        ValueIndex<byte[]> stored = getOrCreateFieldNameBlob("fieldName_" + fieldNameOrdinal);
        AtomicBoolean canceled = new AtomicBoolean(false);
        stored.get(keyStream -> {
            int i = 0;
            for (Long idx : allocateIdxs(externalIds, guidToIdx)) {
                keyStream.key(i, UIO.longBytes(idx), 0, 8);
                i++;
                if (canceled.get()) {
                    return false;
                }
            }
            return true;
        }, (index, key, timestamp, tombstoned, version, payload) -> {
            if (!tombstoned && payload != null) {
                try {
                    if (!values.value(index, payload)) {
                        canceled.set(true);
                        return false;
                    }
                } catch (CancellationException ce) {
                    canceled.set(true);
                    return false;
                } catch (Exception x) {
                    LOG.error("values callback failed.", x);
                    return false;
                }
            }
            return true;
        }, true);
    }


    public void storedValues(RoaringBitmap all, int fieldNameOrdinal, StreamValues values) throws Exception {
        ValueIndex<byte[]> stored = getOrCreateFieldNameBlob("fieldName_" + fieldNameOrdinal);
        AtomicBoolean canceled = new AtomicBoolean(false);
        stored.get(keyStream -> {
            for (Integer idx : all) {
                keyStream.key(idx, UIO.longBytes(idx), 0, 8);
                if (canceled.get()) {
                    return false;
                }
            }
            return true;
        }, (index, key, timestamp, tombstoned, version, payload) -> {
            if (!tombstoned && payload != null) {
                try {
                    if (!values.value(index, payload)) {
                        canceled.set(true);
                        return false;
                    }
                } catch (CancellationException ce) {
                    canceled.set(true);
                    return false;
                } catch (Exception x) {
                    LOG.error("values callback failed.", x);
                    return false;
                }
            }
            return true;
        }, true);
    }


    private ValueIndex<byte[]> getOrCreateFieldNameBlob(String name) throws Exception {
        ValueIndex<byte[]> got = fieldNameBlob.get(name);
        if (got == null) {
            synchronized (fieldNameBlob) {
                got = fieldNameBlob.get(name);
                if (got == null) {
                    got = valueIndexProvider.buildIndex(root, name);
                    fieldNameBlob.put(name, got);
                }
            }
        }
        return got;
    }


    public void update(LABSearchIndexUpdates updates, boolean delete) throws Exception {

        List<Long> externalIds = Lists.newArrayList(updates.guids);
        long[] internalId = allocateIdxs(externalIds, guidToIdx);
        Map<Long, Integer> guidToInternalId = Maps.newHashMap();
        for (int i = 0; i < internalId.length; i++) {
            guidToInternalId.put(externalIds.get(i), (int) internalId[i]);
        }


        List<Future> futures = Lists.newArrayList();
        futures.add(indexerThreads.submit(() -> indexStoredFieldValues(updates, guidToInternalId)));

        long maxTimestamp = 0;
        for (Long timestamp : updates.updateTimestampMillis.values()) {
            maxTimestamp = Math.max(timestamp, maxTimestamp);
        }

        long ef_maxTimestamp = maxTimestamp;
        for (Entry<Integer, BAHash<List<Long>>> entry : updates.fieldNameFieldValueGuids.entrySet()) {
            int fieldOrdinal = entry.getKey();
            entry.getValue().stream(new Semaphore(1),(key, value) -> {
                if (value != null) {
                    futures.add(indexerThreads.submit(() -> indexBits(guidToInternalId, fieldOrdinal, key, value, ef_maxTimestamp, delete)));
                }
                return true;
            });
        }

        for (Future future : futures) {
            future.get();
        }
    }

    private Boolean indexBits(Map<Long, Integer> guidToInternalId,
        int fieldOrdinal,
        byte[] value,
        List<Long> guids,
        long timestamp,
        boolean remove) throws Exception {

        int size = guids.size();
        int[] internalIds = new int[size];
        for (int i = 0; i < size; i++) {
            internalIds[i] = guidToInternalId.get(guids.get(i));
        }
        if (remove) {
            fieldIndex.remove(fieldOrdinal, false, value, internalIds, timestamp);
        } else {
            fieldIndex.set(fieldOrdinal, false, value, internalIds, null, timestamp);
        }
        return true;
    }

    private Boolean indexStoredFieldValues(LABSearchIndexUpdates update, Map<Long, Integer> guidToInternalId) throws Exception {
        for (Entry<Integer, BAHash<byte[]>> entry : update.fieldNameGuidStoredFieldValue.entrySet()) {
            int fieldNameOrdinal = entry.getKey();
            ValueIndex<byte[]> stored = getOrCreateFieldNameBlob("fieldName_" + fieldNameOrdinal);
            stored.append(appendValueStream -> {
                entry.getValue().stream(new Semaphore(1), (key, value) -> {

                    long timestamp = update.updateTimestampMillis.getOrDefault(UIO.bytesLong(key), System.currentTimeMillis());
                    Integer id = guidToInternalId.get(UIO.bytesLong(key));
                    appendValueStream.stream(0, UIO.longBytes(id),
                        timestamp,
                        value == null,
                        timestamp,
                        value);

                    return true;
                });
                return true;
            }, false, new BolBuffer(), new BolBuffer());
        }
        return true;
    }


    private long[] allocateIdxs(Collection<Long> externalIds, ValueIndex<byte[]> externalIdToInternalIdx) throws Exception {
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
                    appendValueStream.stream(0, UIO.longBytes(Long.MAX_VALUE), timestamp, false, timestamp, UIO.longBytes(maxId.get()));
                    int i = 0;
                    for (Long externalId : externalIds) {
                        appendValueStream.stream(i, UIO.longBytes(externalId), timestamp, false, timestamp, UIO.longBytes(internalIds[i]));
                        i++;
                    }
                    return true;
                }, true, new BolBuffer(), new BolBuffer());
            }
        }
        return internalIds;
    }

    public void flush() throws Exception {
        for (Entry<String, ValueIndex<byte[]>> entry : fieldNameBlob.entrySet()) {
            entry.getValue().commit(true, true);
        }

        guidToIdx.commit(true, true);
        fieldIndex.flush();
        fieldNameToFieldId.commit(true, true);
    }


    public void compact() throws Exception {
        for (Entry<String, ValueIndex<byte[]>> entry : fieldNameBlob.entrySet()) {
            compact(entry.getValue());
        }

        compact(guidToIdx);
        compact(fieldNameToFieldId);

        fieldIndex.compact();

    }

    private void compact(ValueIndex<byte[]> index) throws Exception {
        List<Future<Object>> futures = index.compact(true, 0, 0, true);
        for (Future<Object> future : (futures != null) ? futures : Collections.<Future<Object>>emptyList()) {
            future.get();
        }
    }

    public long size() {
        return FileUtils.sizeOfDirectory(root);
    }

    public List<String> summary() throws Exception {
        List<String> s = Lists.newArrayList();
        s.add("guid-to-idx: count=" + guidToIdx.count() + " debt=" + guidToIdx.debt());
        s.add("field-name-lut: count=" + fieldNameToFieldId.count() + " debt=" + fieldNameToFieldId.debt());
        for (Entry<String, ValueIndex<byte[]>> stringValueIndexEntry : fieldNameBlob.entrySet()) {
            ValueIndex<byte[]> valueIndex = stringValueIndexEntry.getValue();
            s.add(stringValueIndexEntry.getKey() + ": count=" + valueIndex.count() + " debt=" + valueIndex.debt());
        }
        return s;
    }

    private int lut(ValueIndex<byte[]> lut, byte[] key) throws Exception {
        long[] idx = { -1L };
        lut.get(keyStream -> {
            keyStream.key(0, key, 0, key.length);
            return true;
        }, (int index, BolBuffer key1, long timestamp, boolean tombstoned, long version, BolBuffer payload) -> {
            if (payload != null) {
                idx[index] = payload.getLong(0);
            }
            return true;
        }, true);
        if (idx[0] == -1) {
            lut.rowScan((int index, BolBuffer key1, long timestamp, boolean tombstoned, long version, BolBuffer payload) -> {
                idx[0] = Math.max(idx[0], payload.getLong(0));
                return true;
            }, true);
            idx[0]++;
            long timestamp = System.currentTimeMillis();
            lut.append(appendValueStream -> {
                appendValueStream.stream(0, key, timestamp, false, timestamp, UIO.longBytes(idx[0]));
                return true;
            }, true, new BolBuffer(), new BolBuffer());
        }
        return (int) idx[0];
    }

    public void delete() throws IOException {
        FileUtils.forceDelete(root);
    }
}
