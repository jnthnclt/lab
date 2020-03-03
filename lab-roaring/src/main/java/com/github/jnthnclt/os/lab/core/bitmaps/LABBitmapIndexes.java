package com.github.jnthnclt.os.lab.core.bitmaps;

import com.github.jnthnclt.os.lab.base.BolBuffer;
import com.github.jnthnclt.os.lab.base.UIO;
import com.github.jnthnclt.os.lab.api.ValueIndex;
import com.github.jnthnclt.os.lab.log.LABLogger;
import com.github.jnthnclt.os.lab.log.LABLoggerFactory;
import com.google.common.primitives.Bytes;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;

public class LABBitmapIndexes<BM extends IBM, IBM> {

    private static final LABLogger LOG = LABLoggerFactory.getLogger();;

    private final LABBitmapIndexVersionProvider versionProvider;
    private final LABBitmaps<BM, IBM> bitmaps;
    private final byte[] bitmapPrefix;
    private final ValueIndex<byte[]>[] bitmapIndexes;
    private final byte[] termPrefix;
    private final ValueIndex<byte[]>[] termIndexes;
    private final int termKeyOffset;
    private final byte[] cardinalityPrefix;
    private final ValueIndex<byte[]>[] cardinalities;

    // We could lock on both field + termId for improved hash/striping, but we favor just termId to reduce object creation
    private final LABStripingLocksProvider locksProvider;
    private final LABIndexKeyInterner indexKeyInterner;

    public LABBitmapIndexes(LABBitmapIndexVersionProvider versionProvider,
        LABBitmaps<BM, IBM> bitmaps,
        byte[] bitmapPrefix,
        ValueIndex<byte[]>[] bitmapIndexes,
        byte[] termPrefix,
        ValueIndex<byte[]>[] termIndexes,
        byte[] cardinalityPrefix,
        ValueIndex<byte[]>[] cardinalities,
        LABStripingLocksProvider locksProvider,
        LABIndexKeyInterner indexKeyInterner) throws Exception {

        this.versionProvider = versionProvider;
        this.bitmaps = bitmaps;
        this.bitmapPrefix = bitmapPrefix;
        this.bitmapIndexes = bitmapIndexes;
        this.termPrefix = termPrefix;
        this.termIndexes = termIndexes;
        this.termKeyOffset = termPrefix.length + 4;
        this.cardinalityPrefix = cardinalityPrefix;
        this.cardinalities = cardinalities;
        this.locksProvider = locksProvider;
        this.indexKeyInterner = indexKeyInterner;
    }

    public void compact() throws Exception {
        for (ValueIndex<byte[]> bitmapIndex : bitmapIndexes) {
            compact(bitmapIndex);
        }

        for (ValueIndex<byte[]> termIndex : termIndexes) {
            compact(termIndex);
        }

        for (ValueIndex<byte[]> cardinality : cardinalities) {
            compact(cardinality);
        }
    }

    private void compact(ValueIndex<byte[]> index) throws Exception {
        List<Future<Object>> futures = index.compact(true, 0, 0, true);
        for (Future<Object> future : (futures != null) ? futures : Collections.<Future<Object>>emptyList()) {
            future.get();
        }
    }

    public void flush() throws Exception {
        for (ValueIndex<byte[]> bitmapIndex : bitmapIndexes) {
            bitmapIndex.commit(true, true);
        }

        for (ValueIndex<byte[]> termIndex : termIndexes) {
            termIndex.commit(true, true);
        }

        for (ValueIndex<byte[]> cardinality : cardinalities) {
            cardinality.commit(true, true);
        }
    }

    private ValueIndex<byte[]> getBitmapIndex(int fieldId) {
        return bitmapIndexes[fieldId % bitmapIndexes.length];
    }

    private ValueIndex<byte[]> getTermIndex(int fieldId) {
        return termIndexes[fieldId % termIndexes.length];
    }

    private ValueIndex<byte[]> getCardinalityIndex(int fieldId) {
        return cardinalities[fieldId % cardinalities.length];
    }

    public void set(int fieldId, boolean cardinality, byte[] key, int[] ids, long[] counts, long timestamp) throws Exception {
        getIndex(fieldId, key).set(ids);
        mergeCardinalities(fieldId, cardinality, key, ids, counts, timestamp);
    }

    public void setIfEmpty(int fieldId, boolean cardinality, byte[] key, int id, long count, long timestamp) throws Exception {
        if (getIndex(fieldId, key).setIfEmpty(id)) {
            mergeCardinalities(fieldId, cardinality, key, new int[] { id }, new long[] { count }, timestamp);
        }
    }

    public void remove(int fieldId, boolean cardinality, byte[] key, int[] ids, long timestamp) throws Exception {
        getIndex(fieldId, key).remove(ids);
        mergeCardinalities(fieldId, cardinality, key, ids, cardinality ? new long[ids.length] : null, timestamp);
    }

    public long getApproximateCount(int fieldId) throws Exception {
        return getTermIndex(fieldId).count();
    }

    public void streamTermIdsForField(
        int fieldId,
        List<LABIndexKeyRange> ranges,
        final LABIndexKeyStream termIdStream) throws Exception {

        byte[] fieldIdBytes = intBytes(fieldId);
        if (ranges == null) {
            byte[] from = termIndexPrefixLowerInclusive(fieldIdBytes);
            byte[] to = termIndexPrefixUpperExclusive(fieldIdBytes);
            getTermIndex(fieldId).rangeScan(from, to, (index, key, timestamp, tombstoned, version, payload) -> {
                byte[] keyBytes = key.copy();
                return termIdStream.stream(indexKeyInterner.intern(keyBytes, termKeyOffset, keyBytes.length - termKeyOffset));
            }, true);
        } else {
            for (LABIndexKeyRange range : ranges) {
                byte[] from = range.getStartInclusiveKey() != null
                    ? termIndexKey(fieldIdBytes, range.getStartInclusiveKey())
                    : termIndexPrefixLowerInclusive(fieldIdBytes);
                byte[] to = range.getStopExclusiveKey() != null
                    ? termIndexKey(fieldIdBytes, range.getStopExclusiveKey())
                    : termIndexPrefixUpperExclusive(fieldIdBytes);
                getTermIndex(fieldId).rangeScan(from, to, (index, key, timestamp, tombstoned, version, payload) -> {
                    byte[] keyBytes = key.copy();
                    return termIdStream.stream(indexKeyInterner.intern(keyBytes, termKeyOffset, keyBytes.length - termKeyOffset));
                }, true);
            }
        }
    }

    public LABBitmapIndex<BM, IBM> get(int fieldId, byte[] key) throws Exception {
        return getIndex(fieldId, key);
    }

    private byte[] bitmapIndexKey(byte[] fieldIdBytes, byte[] termIdBytes) {
        byte[] termLength = new byte[2];
        UIO.shortBytes((short) (termIdBytes.length & 0xFFFF), termLength, 0);
        return Bytes.concat(bitmapPrefix, fieldIdBytes, termLength, termIdBytes);
    }

    private byte[] termIndexKey(byte[] fieldIdBytes, byte[] termIdBytes) {
        return Bytes.concat(termPrefix, fieldIdBytes, termIdBytes);
    }

    private byte[] termIndexPrefixLowerInclusive(byte[] fieldIdBytes) {
        return Bytes.concat(termPrefix, fieldIdBytes);
    }

    private byte[] termIndexPrefixUpperExclusive(byte[] fieldIdBytes) {
        byte[] bytes = termIndexPrefixLowerInclusive(fieldIdBytes);
        makeUpperExclusive(bytes);
        return bytes;
    }

    private static void makeUpperExclusive(byte[] raw) {
        // given: [64,72,96,0] want: [64,72,97,1]
        // given: [64,72,96,127] want: [64,72,96,-128] because -128 is the next lex value after 127
        // given: [64,72,96,-1] want: [64,72,97,0] because -1 is the lex largest value and we roll to the next digit
        for (int i = raw.length - 1; i >= 0; i--) {
            if (raw[i] == -1) {
                raw[i] = 0;
            } else if (raw[i] == Byte.MAX_VALUE) {
                raw[i] = Byte.MIN_VALUE;
                break;
            } else {
                raw[i]++;
                break;
            }
        }
    }

    private byte[] cardinalityIndexKey(byte[] fieldIdBytes, int id, byte[] termIdBytes) {
        return Bytes.concat(cardinalityPrefix, fieldIdBytes, intBytes(id), termIdBytes);
    }

    private LABBitmapIndex<BM, IBM> getIndex(int fieldId, byte[] key) throws Exception {
        byte[] fieldIdBytes = intBytes(fieldId);

        return new LABBitmapIndex<>(versionProvider,
            bitmaps,
            fieldId,
            bitmapIndexKey(fieldIdBytes, key),
            getBitmapIndex(fieldId),
            termIndexKey(fieldIdBytes, key),
            getTermIndex(fieldId),
            locksProvider.lock(key, 0)
        );
    }

    public boolean contains(int fieldId, byte[] key) throws Exception {
        boolean[] exists = { false };
        byte[] termKeyBytes = termIndexKey(intBytes(fieldId), key);
        getTermIndex(fieldId).get(keyStream -> keyStream.key(-1, termKeyBytes, 0, termKeyBytes.length),
            (index1, key1, timestamp1, tombstoned, version1, payload) -> {
                exists[0] = timestamp1 > 0 && !tombstoned;
                return true;
            }, false);
        return exists[0];
    }

//    public void multiGet(String name, int fieldId, LABIndexKey[] indexKeys, LABBitmapAndLastId<BM>[] results) throws Exception {
//        byte[] fieldIdBytes = intBytes(fieldId);
//        ValueIndex<byte[]> bitmapIndex = getBitmapIndex(fieldId);
//        LABReusableByteBufferDataInput in = new LABReusableByteBufferDataInput();
//        for (int i = 0; i < indexKeys.length; i++) {
//            if (indexKeys[i] != null) {
//                byte[] termBytes = indexKeys[i].getBytes();
//                LABBitmapAndLastId<BM> bitmapAndLastId = new LABBitmapAndLastId<>();
//                bitmaps.deserializeAtomized(
//                    bitmapAndLastId,
//                    atomStream -> {
//                        byte[] from = bitmapIndexKey(fieldIdBytes, termBytes);
//                        byte[] to = LABUtils.prefixUpperExclusive(from);
//                        return bitmapIndex.rangeScan(from, to,
//                            (index1, key, timestamp, tombstoned, version, payload) -> {
//                                if (payload != null) {
//                                    int labKey = LABBitmapIndex.deatomize(key.asByteBuffer());
//                                    in.setBuffer(payload.asByteBuffer());
//                                    return atomStream.stream(labKey, in);
//                                }
//                                return true;
//                            },
//                            true);
//                    });
//                results[i] = bitmapAndLastId.isSet() ? bitmapAndLastId : null;
//            }
//        }
//    }
//
//    public void multiGetLastIds(String name, int fieldId, LABIndexKey[] indexKeys, int[] results) throws Exception {
//        byte[] fieldIdBytes = intBytes(fieldId);
//        ValueIndex<byte[]> bitmapIndex = getBitmapIndex(fieldId);
//        LABReusableByteBufferDataInput in = new LABReusableByteBufferDataInput();
//        int[] lastId = new int[1];
//        for (int i = 0; i < indexKeys.length; i++) {
//            if (indexKeys[i] != null) {
//                lastId[0] = -1;
//                byte[] from = bitmapIndexKey(fieldIdBytes, indexKeys[i].getBytes());
//                byte[] to = LABUtils.prefixUpperExclusive(from);
//                bitmapIndex.rangeScan(from, to,
//                    (index, key, timestamp, tombstoned, version, payload) -> {
//                        if (payload != null) {
//                            if (lastId[0] == -1) {
//                                int labKey = LABBitmapIndex.deatomize(key.asByteBuffer());
//                                lastId[0] = LABBitmapIndex.deserLastId(bitmaps, labKey, in, payload.asByteBuffer());
//                                if (lastId[0] != -1) {
//                                    return false;
//                                }
//                            } else {
//                                LOG.warn("Atomized multiGetLastIds failed to halt a range scan");
//                            }
//                        }
//                        return true;
//                    },
//                    true);
//                results[i] = lastId[0];
//            }
//        }
//    }
//
//    public void multiTxIndex(String name,
//        int fieldId,
//        LABIndexKey[] indexKeys,
//        int considerIfLastIdGreaterThanN,
//        LABMultiBitmapIndexTx<IBM> indexTx) throws Exception {
//
//        byte[] fieldIdBytes = intBytes(fieldId);
//        ValueIndex<byte[]> bitmapIndex = getBitmapIndex(fieldId);
//        int[] lastId = new int[1];
//        LABBitmapAndLastId<BM> container = new LABBitmapAndLastId<>();
//        LABReusableByteBufferDataInput in = new LABReusableByteBufferDataInput();
//        for (int i = 0; i < indexKeys.length; i++) {
//            if (indexKeys[i] != null) {
//                container.clear();
//                lastId[0] = -1;
//                byte[] termBytes = indexKeys[i].getBytes();
//                byte[] from = bitmapIndexKey(fieldIdBytes, termBytes);
//                byte[] to = LABUtils.prefixUpperExclusive(from);
//
//                bitmaps.deserializeAtomized(
//                    container,
//                    atomStream -> {
//                        return bitmapIndex.rangeScan(from, to,
//                            (index, key, timestamp, tombstoned, version, payload) -> {
//                                if (payload != null) {
//                                    int labKey = LABBitmapIndex.deatomize(key.asByteBuffer());
//                                    if (lastId[0] == -1) {
//                                        lastId[0] = LABBitmapIndex.deserLastId(bitmaps, labKey, in, payload.asByteBuffer());
//                                        if (lastId[0] != -1 && lastId[0] < considerIfLastIdGreaterThanN) {
//                                            return false;
//                                        }
//                                    }
//                                    in.setBuffer(payload.asByteBuffer());
//                                    return atomStream.stream(labKey, in);
//                                }
//                                return true;
//                            },
//                            true);
//                    });
//
//                if (container.isSet() && (considerIfLastIdGreaterThanN < 0 || lastId[0] > considerIfLastIdGreaterThanN)) {
//                    indexTx.tx(i, container.getLastId(), container.getBitmap(), null, -1);
//                }
//            }
//        }
//    }

    public long getCardinality(int fieldId, boolean cardinality, LABIndexKey indexKey, int id) throws Exception {
        if (cardinality) {
            byte[] fieldIdBytes = intBytes(fieldId);
            long[] count = { 0 };
            byte[] cardinalityIndexKey = cardinalityIndexKey(fieldIdBytes, id, indexKey.getBytes());
            getCardinalityIndex(fieldId).get((streamKeys) -> streamKeys.key(0, cardinalityIndexKey, 0, cardinalityIndexKey.length),
                (index, key, timestamp, tombstoned, version, payload) -> {
                    if (payload != null && !tombstoned) {
                        count[0] = payload.getLong(0);
                    }
                    return false;
                }, true);
            return count[0];
        }
        return -1;
    }

    public long[] getCardinalities(int fieldId, boolean cardinality, LABIndexKey indexKey, int[] ids) throws Exception {
        long[] counts = new long[ids.length];
        if (cardinality) {
            byte[] fieldIdBytes = intBytes(fieldId);
            ValueIndex<byte[]> cardinalityIndex = getCardinalityIndex(fieldId);

            cardinalityIndex.get(
                (stream) -> {
                    for (int i = 0; i < ids.length; i++) {
                        if (ids[i] >= 0) {
                            byte[] cardinalityIndexKey = cardinalityIndexKey(fieldIdBytes, ids[i], indexKey.getBytes());
                            if (!stream.key(i, cardinalityIndexKey, 0, cardinalityIndexKey.length)) {
                                return false;
                            }
                        }
                    }
                    return true;
                },
                (index, key, timestamp, tombstoned, version, payload) -> {
                    if (payload != null && !tombstoned) {
                        counts[index] = payload.getLong(0);
                    }
                    return true;
                },
                true);
        } else {
            Arrays.fill(counts, -1);
        }
        return counts;
    }

    public long getGlobalCardinality(int fieldId, boolean cardinality, LABIndexKey indexKey) throws Exception {
        return getCardinality(fieldId, cardinality, indexKey, -1);
    }

    private void mergeCardinalities(int fieldId, boolean cardinality, byte[] indexKey, int[] ids, long[] counts, long timestamp) throws Exception {
        if (cardinality && counts != null) {
            byte[] fieldBytes = intBytes(fieldId);
            ValueIndex<byte[]> cardinalityIndex = getCardinalityIndex(fieldId);

            long[] merge = new long[counts.length];
            long delta = 0;
            //System.arraycopy(counts, 0, merged, 0, counts.length);

            cardinalityIndex.get(
                keyStream -> {
                    for (int i = 0; i < ids.length; i++) {
                        byte[] key = cardinalityIndexKey(fieldBytes, ids[i], indexKey);
                        if (!keyStream.key(i, key, 0, key.length)) {
                            return false;
                        }
                    }
                    return true;
                },
                (index, key, timestamp1, tombstoned, version, payload) -> {
                    if (payload != null && !tombstoned) {
                        merge[index] = payload.getLong(0);
                    }
                    return false;
                },
                true);

            for (int i = 0; i < ids.length; i++) {
                delta += counts[i] - merge[i];
            }

            long[] globalCount = { 0 };
            byte[] cardinalityIndexKey = cardinalityIndexKey(fieldBytes, -1, indexKey);
            cardinalityIndex.get(
                (keyStream) -> keyStream.key(0, cardinalityIndexKey, 0, cardinalityIndexKey.length),
                (index, key, timestamp1, tombstoned, version, payload) -> {
                    if (payload != null && !tombstoned) {
                        globalCount[0] = payload.getLong(0);
                    }
                    return false;
                },
                true);
            globalCount[0] += delta;

            long version = versionProvider.nextId();
            cardinalityIndex.append(
                valueStream -> {
                    for (int i = 0; i < ids.length; i++) {
                        byte[] key = cardinalityIndexKey(fieldBytes, ids[i], indexKey);
                        if (!valueStream.stream(-1, key, timestamp, false, version, UIO.longBytes(counts[i]))) {
                            return false;
                        }
                    }

                    byte[] globalKey = cardinalityIndexKey(fieldBytes, -1, indexKey);
                    valueStream.stream(-1, globalKey, timestamp, false, version, UIO.longBytes(globalCount[0]));
                    return true;
                },
                true,
                new BolBuffer(),
                new BolBuffer());
        }
    }

    private static byte[] intBytes(int v) {
        return intBytes(v, new byte[4], 0);
    }

    private static byte[] intBytes(int v, byte[] _bytes, int _offset) {
        _bytes[_offset + 0] = (byte) (v >>> 24);
        _bytes[_offset + 1] = (byte) (v >>> 16);
        _bytes[_offset + 2] = (byte) (v >>> 8);
        _bytes[_offset + 3] = (byte) v;
        return _bytes;
    }

}
