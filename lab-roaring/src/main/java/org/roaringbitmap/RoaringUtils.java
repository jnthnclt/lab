package org.roaringbitmap;

public class RoaringUtils {
    public static boolean mightAnd(RoaringBitmap[] bitmaps, RoaringBitmap mask) {

        int c = 1;
        if (mask != null) {
            for (int i = 0; i < bitmaps.length - 1; i++) {
                if (anyAnd(mask, bitmaps[i + 1])) {
                    c++;
                }
            }
            if (c != bitmaps.length) {
                return false;
            }
        }
        c = 1;
        for (int i = 0; i < bitmaps.length - 1; i++) {
            if (anyAnd(bitmaps[i], bitmaps[i + 1])) {
                c++;
            }
        }
        return c == bitmaps.length;

    }

    public static boolean anyAnd(RoaringBitmap x1, RoaringBitmap x2) {
        final int length1 = x1.highLowContainer.size(), length2 = x2.highLowContainer.size();
        int pos1 = 0, pos2 = 0;

        while (pos1 < length1 && pos2 < length2) {
            final char k1 = x1.highLowContainer.getKeyAtIndex(pos1);
            final char k2 = x2.highLowContainer.getKeyAtIndex(pos2);
            if (k1 == k2) {
                final Container c1 = x1.highLowContainer.getContainerAtIndex(pos1);
                final Container c2 = x2.highLowContainer.getContainerAtIndex(pos2);
                final Container c = c1.and(c2);
                if (c.getCardinality() > 0) {
                    return true;
                }
                ++pos1;
                ++pos2;
            } else if (k1 < k2) {
                pos1 = x1.highLowContainer.advanceUntil(k2, pos1);
            } else { // k1 > k2
                pos2 = x2.highLowContainer.advanceUntil(k1, pos2);
            }
        }
        return false;
    }
}
