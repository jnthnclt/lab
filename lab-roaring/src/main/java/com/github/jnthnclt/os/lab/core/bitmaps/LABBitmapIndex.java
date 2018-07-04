package com.github.jnthnclt.os.lab.core.bitmaps;

import com.github.jnthnclt.os.lab.base.BolBuffer;
import com.github.jnthnclt.os.lab.base.IndexUtil;
import com.github.jnthnclt.os.lab.core.LABUtils;
import com.github.jnthnclt.os.lab.core.api.ValueIndex;
import com.github.jnthnclt.os.lab.log.LABLogger;
import com.github.jnthnclt.os.lab.log.LABLoggerFactory;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.roaringbitmap.LABBitmapAndLastId;

/**
 * Created by jonathan.colt on 6/28/17.
 */
public class LABBitmapIndex<BM extends IBM, IBM> {

    private static final LABLogger LOG = LABLoggerFactory.getLogger();

    private final LABBitmapIndexVersionProvider versionProvider;
    private final LABBitmaps<BM, IBM> bitmaps;
    private final int fieldId;
    private final byte[] bitmapKeyBytes;
    private final ValueIndex<byte[]> bitmapIndex;
    private final byte[] termKeyBytes;
    private final ValueIndex<byte[]> termIndex;
    private final Object mutationLock;
    private volatile int lastId = Integer.MIN_VALUE;

    public LABBitmapIndex(LABBitmapIndexVersionProvider versionProvider,
        LABBitmaps<BM, IBM> bitmaps,
        int fieldId,
        byte[] bitmapKeyBytes,
        ValueIndex<byte[]> bitmapIndex,
        byte[] termKeyBytes,
        ValueIndex<byte[]> termIndex,
        Object mutationLock) {

        this.versionProvider = versionProvider;
        this.bitmaps = bitmaps;
        this.fieldId = fieldId;
        this.bitmapKeyBytes = Preconditions.checkNotNull(bitmapKeyBytes);
        this.bitmapIndex = Preconditions.checkNotNull(bitmapIndex);
        this.termKeyBytes = termKeyBytes;
        this.termIndex = termIndex;
        this.mutationLock = mutationLock;
    }

    public int getFieldId() {
        return fieldId;
    }

    public LABBitmapAndLastId<BM> getIndex(LABBitmapAndLastId<BM> container) throws Exception {
        return getIndexInternal(null, container);
    }

    private LABBitmapAndLastId<BM> getIndexInternal(int[] keys, LABBitmapAndLastId<BM> container) throws Exception {
        container.clear();
        LABReusableByteBufferDataInput in = new LABReusableByteBufferDataInput();
        bitmaps.deserializeAtomized(
            container,
            atomStream -> {
                if (keys != null) {
                    int[] atoms = { 0 };
                    bitmapIndex.get(
                        keyStream -> {
                            for (int key : keys) {
                                byte[] keyBytes = atomize(bitmapKeyBytes, key);
                                if (!keyStream.key(-1, keyBytes, 0, keyBytes.length)) {
                                    return false;
                                }
                            }
                            return true;
                        },
                        (index, key, timestamp, tombstoned, version, payload) -> {
                            if (payload != null) {
                                int labKey = deatomize(key.asByteBuffer());
                                if (labKey != Short.MAX_VALUE) {
                                    atoms[0]++;
                                    in.setBuffer(payload.asByteBuffer());
                                    return atomStream.stream(labKey, in);
                                }
                            }
                            return true;
                        },
                        true);
                } else {
                    byte[] from = atomize(bitmapKeyBytes, Short.MAX_VALUE);
                    byte[] to = atomize(LABUtils.prefixUpperExclusive(bitmapKeyBytes), Short.MAX_VALUE);
                    int[] atoms = { 0 };
                    bitmapIndex.pointRangeScan(from, to,
                        (index, key, timestamp, tombstoned, version, payload) -> {
                            if (payload != null) {
                                int labKey = deatomize(key.asByteBuffer());
                                if (labKey != Short.MAX_VALUE) {
                                    atoms[0]++;
                                    in.setBuffer(payload.asByteBuffer());
                                    return atomStream.stream(labKey, in);
                                }
                            }
                            return true;
                        },
                        true);
                }
                return true;
            });

        if (container.isSet()) {
            if (lastId == Integer.MIN_VALUE) {
                lastId = container.getLastId();
            }
        } else {
            lastId = -1;
        }
        return container;
    }

    public <R> R txIndex(LABIndexTx<R, IBM> tx) throws Exception {
        AtomicLong bytes = new AtomicLong();
        R result;
        LABBitmapAndLastId<BM> container = new LABBitmapAndLastId<>();
        LABReusableByteBufferDataInput in = new LABReusableByteBufferDataInput();
        bitmaps.deserializeAtomized(
            container,
            atomStream -> {

                byte[] from = atomize(bitmapKeyBytes, Short.MAX_VALUE);
                byte[] to = atomize(LABUtils.prefixUpperExclusive(bitmapKeyBytes), Short.MAX_VALUE);
                int[] atoms = { 0 };
                bitmapIndex.pointRangeScan(from, to,
                    (index, key, timestamp, tombstoned, version, payload) -> {
                        if (payload != null) {
                            int labKey = deatomize(key.asByteBuffer());
                            if (labKey != Short.MAX_VALUE) {
                                bytes.addAndGet(payload.length);
                                atoms[0]++;
                                in.setBuffer(payload.asByteBuffer());
                                return atomStream.stream(labKey, in);
                            }
                        }
                        return true;
                    },
                    true);
                return true;
            });
        result = tx.tx(container.getBitmap());
        return result;
    }

    public static <BM extends IBM, IBM> int deserLastId(LABBitmaps<BM, IBM> bitmaps,
        int key,
        LABReusableByteBufferDataInput in,
        ByteBuffer byteBuffer) throws IOException {

        byteBuffer.clear();
        in.setBuffer(byteBuffer);
        return bitmaps.lastIdAtomized(in, key);
    }

    public static byte[] atomize(byte[] indexKeyBytes, int key) {
        byte[] atom = new byte[indexKeyBytes.length + 2];
        System.arraycopy(indexKeyBytes, 0, atom, 0, indexKeyBytes.length);
        short reversed = (short) ((0xFFFF - key) & 0xFFFF);
        atom[atom.length - 2] = (byte) (reversed >>> 8);
        atom[atom.length - 1] = (byte) reversed;
        return atom;
    }

//    public static int deatomize(byte[] key) {
//        int v = 0;
//        v |= (key[key.length - 2] & 0xFF);
//        v <<= 8;
//        v |= (key[key.length - 1] & 0xFF);
//        return (0xFFFF - v);
//    }

    public static int deatomize(ByteBuffer key) {
        key.clear();
        int v = 0;
        v |= (key.get(key.capacity() - 2) & 0xFF);
        v <<= 8;
        v |= (key.get(key.capacity() - 1) & 0xFF);
        return (0xFFFF - v);
    }

    public static void main(String[] args) {

        //for (int i = 0; i <= Short.MAX_VALUE; i+=1024) {
            byte[] atomize = atomize(new byte[] { 0 }, Short.MAX_VALUE);
            //byte[] upperExclusive = atomize(LABUtils.prefixUpperExclusive(new byte[] { 0 }),Short.MAX_VALUE);
            byte[] upperExclusive = atomize(new byte[] { 1 }, Short.MAX_VALUE);

        System.out.println(Arrays.toString(atomize));
        System.out.println(Arrays.toString(upperExclusive));

//            System.out.println(i+" "+Arrays.toString(atomize));
//            System.out.println(i+" "+Arrays.toString(upperExclusive));

            System.out.println(IndexUtil.compare(
                atomize,0,3,
                upperExclusive, 0, 3
                ));
        //}
    }

    private BM getOrCreateIndex(int[] keys) throws Exception {
        LABBitmapAndLastId<BM> index = new LABBitmapAndLastId<>();
        getIndexInternal(keys, index);
        BM bitmap = index.isSet() ? index.getBitmap() : bitmaps.create();
        return bitmap;
    }

    private byte[][] keyBytes(int[] keys, IBM index) throws Exception {
        byte[][] bytes;
        long[] sizes = bitmaps.serializeAtomizedSizeInBytes(index, keys);
        ByteArrayDataOutput[] dataOutputs = new ByteArrayDataOutput[keys.length];
        for (int i = 0; i < keys.length; i++) {
            dataOutputs[i] = sizes[i] < 0 ? null : ByteStreams.newDataOutput((int) sizes[i]);
        }
        bitmaps.serializeAtomized(index, keys, dataOutputs);
        bytes = new byte[keys.length][];
        for (int i = 0; i < keys.length; i++) {
            bytes[i] = dataOutputs[i] == null ? null : dataOutputs[i].toByteArray();
        }
        return bytes;
    }

    private void setIndex(int[] keys, IBM index) throws Exception {
        bitmaps.optimize(index, keys);
        byte[][] bytes = keyBytes(keys, index);

        long timestamp = System.currentTimeMillis();
        long version = versionProvider.nextId();
        if (termIndex != null) {
            boolean[] exists = { false };
            termIndex.get(keyStream -> keyStream.key(-1, termKeyBytes, 0, termKeyBytes.length),
                (index1, key, timestamp1, tombstoned, version1, payload) -> {
                    exists[0] = timestamp1 > 0 && !tombstoned;
                    return true;
                }, false);
            if (!exists[0]) {
                termIndex.append(
                    stream -> {
                        if (!stream.stream(-1, termKeyBytes, timestamp, false, version, null)) {
                            return false;
                        }
                        return true;
                    },
                    true,
                    new BolBuffer(),
                    new BolBuffer());
            }
        }

        bitmapIndex.append(
            stream -> {
                if (!stream.stream(-1, atomize(bitmapKeyBytes, Short.MAX_VALUE), timestamp, false, version, new byte[0])) {
                    return false;
                }
                for (int i = 0; i < keys.length; i++) {
                    if (!stream.stream(-1, atomize(bitmapKeyBytes, keys[i]), timestamp, false, version, bytes[i])) {
                        return false;
                    }
                }
                return true;
            },
            true,
            new BolBuffer(),
            new BolBuffer());

        lastId = bitmaps.lastSetBit(index);

    }

    /*private int[] keysFromIds(int... ids) {
        TIntSet keySet = new TIntHashSet();
        for (int id : ids) {
            keySet.add(bitmaps.key(id));
        }
        int[] keys = keySet.toArray();
        Arrays.sort(keys);
        return keys;
    }*/


    private int[] keysFromIds(int... ids) {
        if (ids.length == 0) {
            return ids;
        }

        int[] ks = new int[ids.length];
        for (int i = 0; i < ids.length; i++) {
            ks[i] = bitmaps.key(ids[i]);
        }

        return orderedSet(ks);
    }

    private int[] orderedSet(int[] ks) {
        Arrays.sort(ks);
        int l = 0;
        for (int i = 0; i < ks.length; i++) {
            if (ks[l] == ks[i]) {
                ks[l] = ks[i];
            } else {
                l++;
                ks[l] = ks[i];
            }
        }
        l++;
        if (l == ks.length) {
            return ks;
        } else {
            int[] keys = new int[l];
            System.arraycopy(ks, 0, keys, 0, l);
            return keys;
        }
    }


    public void remove(int... ids) throws Exception {
        synchronized (mutationLock) {
            int[] keys = keysFromIds(ids);
            BM index = getOrCreateIndex(keys);
            BM r = bitmaps.remove(index, ids);
            setIndex(keys, r);
        }
    }

    public void set(int... ids) throws Exception {
        if (ids.length == 0) {
            return;
        }
        synchronized (mutationLock) {
            int[] keys = keysFromIds(ids);
            BM index = getOrCreateIndex(keys);
            BM r = bitmaps.set(index, ids);
            setIndex(keys, r);
        }
    }

    public boolean setIfEmpty(int id) throws Exception {
        synchronized (mutationLock) {
            int lastId = lastId();
            if (lastId < 0) {
                set(id);
                return true;
            }
        }
        return false;
    }

    public int lastId() throws Exception {
        if (lastId == Integer.MIN_VALUE) {
            AtomicLong bytes = new AtomicLong();
            synchronized (mutationLock) {
                int[] id = { -1 };
                LABReusableByteBufferDataInput in = new LABReusableByteBufferDataInput();
                byte[] from = atomize(bitmapKeyBytes, Short.MAX_VALUE);
                byte[] to = atomize(LABUtils.prefixUpperExclusive(bitmapKeyBytes), Short.MAX_VALUE);
                bitmapIndex.pointRangeScan(from, to,
                    (index, key, timestamp, tombstoned, version, payload) -> {
                        if (payload != null) {
                            if (id[0] == -1) {
                                bytes.addAndGet(payload.length);
                                int labKey = LABBitmapIndex.deatomize(key.asByteBuffer());
                                if (labKey != Short.MAX_VALUE) {
                                    id[0] = LABBitmapIndex.deserLastId(bitmaps, labKey, in, payload.asByteBuffer());
                                    if (id[0] != -1) {
                                        return false;
                                    }
                                }
                            } else {
                                LOG.warn("Atomized multiGetLastIds failed to halt a range scan");
                            }
                        }
                        return true;
                    },
                    true);
                lastId = id[0];
            }
        }
        return lastId;
    }

    public void andNot(IBM mask) throws Exception {
        synchronized (mutationLock) {
            int[] keys = bitmaps.keys(mask);
            BM index = getOrCreateIndex(keys);
            BM r = bitmaps.andNot(index, mask);
            int[] delta = bitmaps.keysNotEqual(r, index);
            setIndex(delta, r);
        }
    }

    public void or(IBM mask) throws Exception {
        synchronized (mutationLock) {
            int[] keys = bitmaps.keys(mask);
            BM index = getOrCreateIndex(keys);
            BM r = bitmaps.or(Arrays.asList(index, mask));
            int[] delta = bitmaps.keysNotEqual(r, index);
            setIndex(delta, r);
        }
    }

//    public void andNotToSourceSize(List<IBM> masks) throws Exception {
//        synchronized (mutationLock) {
//            TIntSet keySet = new TIntHashSet();
//            for (IBM mask : masks) {
//                keySet.addAll(bitmaps.keys(mask));
//            }
//            int[] keys = keySet.toArray();
//            Arrays.sort(keys);
//            BM index = getOrCreateIndex(keys);
//            BM andNot = bitmaps.andNotToSourceSize(index, masks);
//            int[] delta = bitmaps.keysNotEqual(andNot, index);
//            setIndex(delta, andNot);
//        }
//    }


    public void andNotToSourceSize(List<IBM> masks) throws Exception {
        synchronized (mutationLock) {
            int[][] ids = new int[masks.size()][];
            int i = 0;
            for (IBM mask : masks) {
                ids[i] = bitmaps.keys(mask);
                i++;
            }
            int[] keys = orderedSet(Ints.concat(ids));
            BM index = getOrCreateIndex(keys);
            BM andNot = bitmaps.andNotToSourceSize(index, masks);
            int[] delta = bitmaps.keysNotEqual(andNot, index);
            setIndex(delta, andNot);
        }
    }


    public void orToSourceSize(IBM mask) throws Exception {
        synchronized (mutationLock) {
            int[] keys = bitmaps.keys(mask);
            BM index = getOrCreateIndex(keys);
            BM or = bitmaps.orToSourceSize(index, mask);
            int[] delta = bitmaps.keysNotEqual(or, index);
            setIndex(delta, or);
        }
    }
}
