package com.github.jnthnclt.os.lab.core.sort;

import org.roaringbitmap.RoaringBitmap;

import java.util.concurrent.atomic.AtomicInteger;

class BitSortTree {


    final BitSort bitSort;
    private final AtomicInteger i = new AtomicInteger(0);


    public BitSortTree(int count, int leafSize) {
        this.bitSort = new BitSort(count, leafSize);
    }

    /**
     * The expectation is that internalIds are added in sort order
     *
     * @param internalId
     */
    public void add(int internalId, int value) {
        if (i.get() < 0) {
            throw new IllegalStateException("Cannot add after calling done.");
        }
        bitSort.add(i.getAndIncrement(), internalId, value);
    }

    public void done() {
        i.set(-1);
        bitSort.done();
    }

    public RoaringBitmap topN(RoaringBitmap answer, int limit) {
        if (i.get() != -1) {
            throw new IllegalStateException("Cannot get topN until done is called.");
        }
        RoaringBitmap keep = new RoaringBitmap();
        bitSort.topN(answer, keep, limit);
        return keep;
    }
}
