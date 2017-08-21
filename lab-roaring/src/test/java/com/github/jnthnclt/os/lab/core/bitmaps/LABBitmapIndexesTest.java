package com.github.jnthnclt.os.lab.core.bitmaps;

import com.github.jnthnclt.os.lab.core.api.ValueIndex;
import java.util.concurrent.atomic.AtomicLong;
import org.roaringbitmap.LABBitmapAndLastId;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

public class LABBitmapIndexesTest {

    @Test
    public void testAppend() throws Exception {
        LABBitmaps<RoaringBitmap, RoaringBitmap> bitmaps = new RoaringLABBitmaps();

        ValueIndex<byte[]> bitmapIndex = LABBitmapIndexTest.buildValueIndex("bitmaps");
        ValueIndex<byte[]> termIndex = LABBitmapIndexTest.buildValueIndex("terms");
        ValueIndex<byte[]> cardinalitiesIndex = LABBitmapIndexTest.buildValueIndex("cardinalities");

        AtomicLong version = new AtomicLong();
        LABBitmapIndexes<RoaringBitmap, RoaringBitmap> index = new LABBitmapIndexes<RoaringBitmap, RoaringBitmap>(
            () -> version.getAndIncrement(),
            bitmaps,
            new byte[] { 0 },
            (ValueIndex<byte[]>[]) new ValueIndex[] { bitmapIndex },
            new byte[] { 1 },
            (ValueIndex<byte[]>[]) new ValueIndex[] { termIndex },
            new byte[] { 2 },
            (ValueIndex<byte[]>[]) new ValueIndex[] { cardinalitiesIndex },
            new LABStripingLocksProvider(64),
            new LABIndexKeyInterner(true));

        LABBitmapIndex<RoaringBitmap, RoaringBitmap> a = index.get(0, "a".getBytes());
        LABBitmapAndLastId<RoaringBitmap> container = new LABBitmapAndLastId<>();
        System.out.println(a.lastId() + " " + a.getIndex(container).getBitmap());
        a.set(1, 3, 5, 7, 9, 10, 11, 13, 15);
        System.out.println("a=" + a.lastId() + " " + a.getIndex(container).getBitmap());

        LABBitmapIndex<RoaringBitmap, RoaringBitmap> b = index.get(0, "b".getBytes());
        System.out.println(b.lastId() + " " + b.getIndex(container).getBitmap());
        b.set(0, 2, 4, 6, 8, 9, 10, 12, 13, 14);
        System.out.println("b=" + b.lastId() + " " + b.getIndex(container).getBitmap());

        RoaringBitmap r = bitmaps.create();
        r.or(a.getIndex(container).getBitmap());
        r.and(b.getIndex(container).getBitmap());


        System.out.println("r=" + r);


        index.streamTermIdsForField(0, null, indexKey -> {
            System.out.println(new String(indexKey.getBytes()));
            return true;
        });

    }
}
