package com.github.jnthnclt.os.lab.collections.oh;

import com.github.jnthnclt.os.lab.collections.KeyValueStream;
import java.util.concurrent.Semaphore;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author jonathan.colt
 */
public class ConcurrentOHash<K, V> {

    private final int capacity;
    private final boolean hasValues;
    private final OHasher<K> hasher;
    private final OHEqualer<K> equaler;

    private final Semaphore[] hmapsSemaphore;
    private final OHash<K, V>[] hmaps;

    @SuppressWarnings("unchecked")
    public ConcurrentOHash(int capacity, boolean hasValues, int concurrency, OHasher<K> hasher, OHEqualer<K> equaler) {
        this.capacity = capacity;
        this.hasValues = hasValues;
        this.hasher = hasher;
        this.equaler = equaler;
        this.hmapsSemaphore = new Semaphore[concurrency];
        this.hmaps = new OHash[concurrency];
    }

    public void put(K key, V value) throws InterruptedException {
        int hashCode = hasher.hashCode(key);
        int i = hmap(hashCode, true);
        OHash<K, V> hmap = hmaps[i];
        hmapsSemaphore[i].acquire(Short.MAX_VALUE);
        try {
            hmap.put(hashCode, key, value);
        } finally {
            hmapsSemaphore[i].release(Short.MAX_VALUE);
        }
    }

    private int hmap(int hashCode, boolean create) {
        int index = Math.abs((hashCode) % hmaps.length);
        if (hmaps[index] == null && create) {
            synchronized (hmaps) {
                if (hmaps[index] == null) {
                    hmapsSemaphore[index] = new Semaphore(Short.MAX_VALUE, true);
                    hmaps[index] = new OHash<>(new OHMapState<>(capacity, hasValues, null), hasher, equaler);
                }
            }
        }
        return index;
    }

    public V computeIfAbsent(K key, Function<K, ? extends V> mappingFunction) throws InterruptedException {
        int hashCode = hasher.hashCode(key);
        int i = hmap(hashCode, true);
        OHash<K, V> hmap = hmaps[i];
        hmapsSemaphore[i].acquire(Short.MAX_VALUE);
        try {
            V value = hmap.get(hashCode, key);
            if (value == null) {
                value = mappingFunction.apply(key);
                hmap.put(hashCode, key, value);
            }
            return value;
        } finally {
            hmapsSemaphore[i].release(Short.MAX_VALUE);
        }
    }

    public V compute(K key, BiFunction<K, ? super V, ? extends V> remappingFunction) throws InterruptedException {
        int hashCode = hasher.hashCode(key);
        int i = hmap(hashCode, true);
        OHash<K, V> hmap = hmaps[i];
        hmapsSemaphore[i].acquire(Short.MAX_VALUE);
        try {
            V value = hmap.get(hashCode, key);
            V remapped = remappingFunction.apply(key, value);
            if (remapped != value) {
                value = remapped;
                hmap.put(hashCode, key, value);
            }
            return value;
        } finally {
            hmapsSemaphore[i].release(Short.MAX_VALUE);
        }
    }

    public V get(K key) throws InterruptedException {
        int hashCode = hasher.hashCode(key);
        int i = hmap(hashCode, false);
        OHash<K, V> hmap = hmaps[i];
        if (hmap != null) {
            hmapsSemaphore[i].acquire(Short.MAX_VALUE);
            try {
                return hmap.get(hashCode, key);
            } finally {
                hmapsSemaphore[i].release(Short.MAX_VALUE);
            }
        }
        return null;
    }

    public void remove(K key) throws InterruptedException {
        int hashCode = hasher.hashCode(key);
        int i = hmap(hashCode, false);
        OHash<K, V> hmap = hmaps[i];
        if (hmap != null) {
            hmapsSemaphore[i].acquire(Short.MAX_VALUE);
            try {
                hmap.remove(hashCode, key);
            } finally {
                hmapsSemaphore[i].release(Short.MAX_VALUE);
            }
        }
    }

    public void clear() throws InterruptedException {
        for (int i = 0; i < hmaps.length; i++) {
            OHash<K, V> hmap = hmaps[i];
            if (hmap != null) {
                hmapsSemaphore[i].acquire(Short.MAX_VALUE);
                try {
                    hmap.clear();
                } finally {
                    hmapsSemaphore[i].release(Short.MAX_VALUE);
                }
            }
        }
    }

    public int size() {
        int size = 0;
        for (OHash<K, V> hmap : hmaps) {
            if (hmap != null) {
                size += hmap.size();
            }
        }
        return size;
    }

    public boolean stream(KeyValueStream<K, V> keyValueStream) throws Exception {
        for (int i = 0; i < hmaps.length; i++) {
            OHash<K, V> hmap = hmaps[i];
            if (hmap != null) {
                if (!hmap.stream(hmapsSemaphore[i], keyValueStream)) {
                    return false;
                }
            }
        }
        return true;
    }

}
