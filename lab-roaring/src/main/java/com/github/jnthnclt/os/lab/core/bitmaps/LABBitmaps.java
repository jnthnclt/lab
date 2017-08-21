package com.github.jnthnclt.os.lab.core.bitmaps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.roaringbitmap.LABBitmapAndLastId;
import org.roaringbitmap.LABStreamAtoms;
import org.roaringbitmap.RoaringBitmap;

/**
 * Created by jonathan.colt on 6/28/17.
 */
public interface LABBitmaps<BM extends IBM, IBM> {

    BM create();

    int[] keysNotEqual(BM or, BM index);

    BM orToSourceSize(BM index, IBM mask);

    int[] keys(IBM mask);

    BM andNotToSourceSize(BM index, List<IBM> masks);

    BM or(Collection<IBM> ibms);

    boolean isSet(RoaringBitmap bitmap, int i);

    BM andNot(BM index, IBM mask);

    BM set(BM index, int[] ids);

    BM remove(BM index, int[] ids);

    int key(int id);

    int lastSetBit(IBM index);

    void optimize(IBM index, int[] keys);

    void serializeAtomized(IBM index, int[] keys, DataOutput[] dataOutputs) throws IOException;

    long[] serializeAtomizedSizeInBytes(IBM index, int[] keys) throws IOException;

    boolean deserializeAtomized(LABBitmapAndLastId<BM> container, LABStreamAtoms streamAtoms) throws IOException;

    int lastIdAtomized(DataInput in, int key) throws IOException;

}
