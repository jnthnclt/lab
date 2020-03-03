package com.github.jnthnclt.os.lab.core.correlate;

import com.google.common.collect.Lists;
import org.roaringbitmap.RoaringBitmap;

import java.util.Arrays;
import java.util.List;

public class FloatBitSort {

    int size;
    int halfSize;
    RoaringBitmap bitmap = new RoaringBitmap();
    public float minValue = Float.MAX_VALUE;
    public float maxValue = -Float.MAX_VALUE;

    FloatBitSort high;
    FloatBitSort low;

    public FloatBitSort smaller;
    public FloatBitSort parent;
    public FloatBitSort bigger;

    public FloatBitSort(int size, int leafSize) {
        this(null, size, leafSize);
    }

    public FloatBitSort(FloatBitSort parent, int size, int leafSize) {
        this.parent = parent;
        this.size = size;
        this.halfSize = size / 2;
        if (this.size > leafSize) {
            high = new FloatBitSort(this, halfSize, leafSize);
            low = new FloatBitSort(this, halfSize, leafSize);
        }
    }

    public void link() {
        List<FloatBitSort> collect = Lists.newArrayList();
        collectLeafs(collect);
        for (int i = 1; i < collect.size(); i++) {
            FloatBitSort before = collect.get(i - 1);
            FloatBitSort after = collect.get(i);
            before.bigger = after;
            after.smaller = before;
        }
    }

    public void add(int order, int id, float value) {

        if (high != null) {
            if (order < halfSize) {
                high.add(order, id, value);
            } else {
                low.add(order - halfSize, id, value);
            }
        } else {
            bitmap.add(id);
        }
        maxValue = Math.max(maxValue, value);
        minValue = Math.min(minValue, value);
    }

    public void collectLeafs(List<FloatBitSort> collect) {
        if (high != null) {
            high.collectLeafs(collect);
            low.collectLeafs(collect);
        } else {
            collect.add(this);
        }
    }

    public FloatBitSort leaf(float value) {
        if (high != null && value >= high.minValue && value <= high.maxValue) {
            return high.leaf(value);
        } else if (low != null && value >= low.minValue && value <= low.maxValue) {
            return low.leaf(value);
        } else {
            if (value >= minValue && value <= maxValue) {
                return this;
            }
        }
        return this;
    }

    public RoaringBitmap bitmap() {
        return bitmap;
    }


    public RoaringBitmap done() {
        if (high != null) {
            bitmap.or(high.done());
            bitmap.or(low.done());
        }
        return bitmap;
    }

    public int topN(RoaringBitmap answer, RoaringBitmap keep, int n) {
        RoaringBitmap and = RoaringBitmap.and(answer, bitmap);
        int cardinality = and.getCardinality();
        if (cardinality > n) {
            if (high != null) {
                int keeping = high.topN(answer, keep, n);
                if (keeping < n) {
                    if (low != null) {
                        keeping = low.topN(answer, keep, n);
                        if (keeping < n) {
                            keep.or(and);
                        }
                    }
                }
            } else {
                keep.or(and);
            }
        } else {
            keep.or(and);
        }
        return keep.getCardinality();
    }

    public void printTree(int depth) {
        if (high != null) {
            System.out.println(pad(depth, ' ') + "high:");
            high.printTree(depth + 1);
            System.out.println(pad(depth, ' ') + "low:");
            low.printTree(depth + 1);
        } else {
            System.out.println(pad(depth, ' ') + "values[" + minValue + ".." + maxValue + "]\n");
        }

    }

    public String pad(int depth, char c) {
        char[] repeat = new char[depth];
        Arrays.fill(repeat, c);
        return new String(repeat);
    }

    @Override
    public String toString() {
        return "BitSort{" +
                "min=" + minValue +
                ", max=" + maxValue +
                '}';
    }
}
