package com.github.jnthnclt.os.lab.core.search;

import com.github.jnthnclt.os.lab.base.UIO;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringUtils;

public class LABSearch {

    public static RoaringBitmap andFieldValues(LABSearchIndex index,
        List<CachedFieldValue> andFieldValue,
        RoaringBitmap resultOfAnds,
        RoaringBitmap mask) throws Exception {

        int maxSize = andFieldValue.size();
        RoaringBitmap[] bitmaps = new RoaringBitmap[maxSize];
        int actualSize = 0;
        for (int i = 0; i < maxSize; i++) {
            CachedFieldValue fieldValue = andFieldValue.get(i);
            if (fieldValue.value != null) {
                if (fieldValue.cache == null) {
                    int fo = index.fieldOrdinal(fieldValue.fieldName);
                    fieldValue.cache = index.bitmap(fo, fieldValue.value);
                    if (fieldValue.cache == null) {
                        fieldValue.cache = new RoaringBitmap();
                    }
                    if (mask != null) {
                        fieldValue.cache.and(mask);
                    }
                }
                bitmaps[actualSize] = fieldValue.cache;
                actualSize++;
            }
        }

        if (actualSize < maxSize) {
            RoaringBitmap[] actualBitmaps = new RoaringBitmap[actualSize];
            System.arraycopy(bitmaps, 0, actualBitmaps, 0, actualSize);
            bitmaps = actualBitmaps;
        }

        if (RoaringUtils.mightAnd(bitmaps, mask)) {
            RoaringBitmap and = FastAggregation.and(bitmaps);
            if (resultOfAnds != null) {
                resultOfAnds.and(and);
                return resultOfAnds;
            } else {
                return and;
            }
        } else {
            return new RoaringBitmap();
        }
    }

    public static class CachedFieldValue {
        public final String fieldName;
        public final byte[] value;
        public RoaringBitmap cache;

        public CachedFieldValue(String fieldName, String value) {
            this(fieldName, value == null ? null : value.getBytes(StandardCharsets.UTF_8));
        }

        public CachedFieldValue(String fieldName, int value) {
            this(fieldName, UIO.intBytes(value, new byte[4], 0));
        }

        public CachedFieldValue(String fieldName, long value) {
            this(fieldName, UIO.longBytes(value, new byte[8], 0));
        }

        public CachedFieldValue(String fieldName, byte[] value) {
            this.fieldName = fieldName;
            this.value = value;
        }

        @Override
        public String toString() {
            return fieldName + ":" + (value == null ? value : new String(value, StandardCharsets.UTF_8));
        }
    }

}
