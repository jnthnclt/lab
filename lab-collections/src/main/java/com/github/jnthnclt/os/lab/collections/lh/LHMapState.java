package com.github.jnthnclt.os.lab.collections.lh;

import java.util.Arrays;

/**
 *
 * @author jonathan.colt
 */
public class LHMapState<V> {

    private static final int MIN_POWER = 1;

    private final int power;
    private final int capacity;
    private final long nilKey;
    private final long skipKey;
    private final long[] keys;
    private final Object[] values;
    private int count;

    public LHMapState(int capacity, long nilKey, long skipKey) {
        this.count = 0;

        int power = chunkPower(capacity);
        capacity = 1 << power;
        this.power = 63 - power;

        this.capacity = capacity;
        this.nilKey = nilKey;
        this.skipKey = skipKey;

        this.keys = new long[(int) capacity];
        Arrays.fill(keys, nilKey);
        this.values = new Object[(int) capacity];
    }

    public int indexForHash(int hash) {
        // fibonacciIndexForHash
        // hash ^= hash >> power;
        // long index = (7540113804746346429L * hash) >> power;
        // return index < 0 ? -index : index;

        hash += hash >> 8;
        if (hash < 0) {
            hash = -hash;
        }
        return hash % capacity;
    }

    private static int chunkPower(int length) {
        if (length == 0) {
            return 0;
        }
        int numberOfTrailingZeros = 64 - Long.numberOfLeadingZeros(length - 1);
        return Math.max(MIN_POWER, numberOfTrailingZeros);
    }

    public LHMapState<V> allocate(int capacity) {
        return new LHMapState<>(capacity, nilKey, skipKey);
    }

    public long skipped() {
        return skipKey;
    }

    public long nil() {
        return nilKey;
    }

    public int first() {
        return 0;
    }

    public int size() {
        return count;
    }

    public void update(int i, long key, V value) {
        keys[i] = key;
        values[i] = value;
    }

    public void link(int i, long key, V value) {
        keys[i] = key;
        values[i] = value;
        count++;
    }

    public void clear(int i) {
        keys[i] = nilKey;
        values[i] = null;
    }

    public void remove(int i, long key, V value) {
        keys[i] = key;
        values[i] = value;
        count--;
    }

    public int next(int i) {
        return (i >= capacity - 1) ? -1 : i + 1;
    }

    public int capacity() {
        return capacity;
    }

    public long key(int i) {
        return keys[i];
    }

    @SuppressWarnings("unchecked")
    public V value(int i) {
        return (V) values[i];
    }
}
