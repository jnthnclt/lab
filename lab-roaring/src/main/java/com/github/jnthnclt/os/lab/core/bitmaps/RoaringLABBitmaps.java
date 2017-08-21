package com.github.jnthnclt.os.lab.core.bitmaps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.LABBitmapAndLastId;
import org.roaringbitmap.LABRoaringInspection;
import org.roaringbitmap.LABStreamAtoms;
import org.roaringbitmap.RoaringBitmap;

/**
 * Created by jonathan.colt on 6/28/17.
 */
public class RoaringLABBitmaps implements LABBitmaps<RoaringBitmap, RoaringBitmap> {

    private boolean addInPlace(RoaringBitmap bitmap, int... indexes) {
        if (indexes.length == 1) {
            bitmap.add(indexes[0]);
        } else if (indexes.length > 1) {
            int rangeStart = 0;
            for (int rangeEnd = 1; rangeEnd < indexes.length; rangeEnd++) {
                if (indexes[rangeEnd - 1] + 1 != indexes[rangeEnd]) {
                    if (rangeStart == rangeEnd - 1) {
                        bitmap.add(indexes[rangeStart]);
                    } else {
                        bitmap.add(indexes[rangeStart], indexes[rangeEnd - 1] + 1);
                    }
                    rangeStart = rangeEnd;
                }
            }
            if (rangeStart == indexes.length - 1) {
                bitmap.add(indexes[rangeStart]);
            } else {
                bitmap.add(indexes[rangeStart], indexes[indexes.length - 1] + 1);
            }
        }
        return true;
    }

    @Override
    public RoaringBitmap set(RoaringBitmap bitmap, int... indexes) {
        RoaringBitmap container = copy(bitmap);
        addInPlace(container, indexes);
        return container;
    }


    @Override
    public RoaringBitmap remove(RoaringBitmap bitmap, int... indexes) {
        RoaringBitmap container = copy(bitmap);
        for (int index : indexes) {
            container.remove(index);
        }
        return container;
    }

    public RoaringBitmap copy(RoaringBitmap original) {
        RoaringBitmap container = new RoaringBitmap();
        container.or(original);
        return container;
    }

    @Override
    public RoaringBitmap create() {
        return new RoaringBitmap();
    }

    @Override
    public boolean isSet(RoaringBitmap bitmap, int i) {
        return bitmap.contains(i);
    }

    @Override
    public RoaringBitmap andNot(RoaringBitmap original, RoaringBitmap bitmap) {
        return RoaringBitmap.andNot(original, bitmap);
    }

    @Override
    public RoaringBitmap orToSourceSize(RoaringBitmap source, RoaringBitmap mask) {
        return or(Arrays.asList(source, mask));
    }

    @Override
    public RoaringBitmap andNotToSourceSize(RoaringBitmap source, List<RoaringBitmap> masks) {
        return andNot(source, masks);
    }

    public RoaringBitmap andNot(RoaringBitmap original, List<RoaringBitmap> bitmaps) {

        if (bitmaps.isEmpty()) {
            return copy(original);
        } else {
            RoaringBitmap container = RoaringBitmap.andNot(original, bitmaps.get(0));
            for (int i = 1; i < bitmaps.size(); i++) {
                container.andNot(bitmaps.get(i));
                if (container.isEmpty()) {
                    break;
                }
            }
            return container;
        }
    }

    @Override
    public int lastSetBit(RoaringBitmap bitmap) {
        return bitmap.isEmpty() ? -1 : bitmap.last();
    }


    @Override
    public int key(int position) {
        return LABRoaringInspection.key(position);
    }

    @Override
    public int[] keys(RoaringBitmap mask) {
        return LABRoaringInspection.keys(mask);
    }

    @Override
    public int[] keysNotEqual(RoaringBitmap a, RoaringBitmap b) {
        return LABRoaringInspection.keysNotEqual(a, b);
    }

    @Override
    public long[] serializeAtomizedSizeInBytes(RoaringBitmap index, int[] keys) {
        return LABRoaringInspection.serializeSizeInBytes(index, keys);
    }

    @Override
    public void serializeAtomized(RoaringBitmap index, int[] keys, DataOutput[] dataOutputs) throws IOException {
        LABRoaringInspection.userializeAtomized(index, keys, dataOutputs);
    }

    @Override
    public boolean deserializeAtomized(LABBitmapAndLastId<RoaringBitmap> container, LABStreamAtoms streamAtoms) throws IOException {
        return LABRoaringInspection.udeserialize(container, streamAtoms);
    }

    @Override
    public void optimize(RoaringBitmap index, int[] keys) {
        LABRoaringInspection.optimize(index, keys);
    }

    @Override
    public RoaringBitmap or(Collection<RoaringBitmap> bitmaps) {
        return FastAggregation.or(bitmaps.iterator());
    }

    @Override
    public int lastIdAtomized(DataInput dataInput, int key) throws IOException {
        return LABRoaringInspection.lastSetBit(key, dataInput);
    }

}
