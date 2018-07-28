package com.github.jnthnclt.os.lab.collections.lh;

import java.util.concurrent.Semaphore;

/**
 * @author jonathan.colt
 */
public class ConcurrentLHash<V> {

    private final int capacity;
    private final long nilKey;
    private final long skipKey;
    private final Semaphore[] hmapsSemaphore;
    private final LHash<V>[] hmaps;

    @SuppressWarnings("unchecked")
    public ConcurrentLHash(int capacity, long nilKey, long skipKey, int concurrency) {
        this.capacity = capacity;
        this.nilKey = nilKey;
        this.skipKey = skipKey;
        this.hmapsSemaphore = new Semaphore[concurrency];
        this.hmaps = new LHash[concurrency];
    }

    public void put(long key, V value) throws InterruptedException {
        int i = hmap(key, true);
        LHash<V> hmap = hmaps[i];
        hmapsSemaphore[i].acquire(Short.MAX_VALUE);
        try {
            hmap.put(key, value);
        } finally {
            hmapsSemaphore[i].release(Short.MAX_VALUE);
        }
    }

    private int hmap(long key, boolean create) {
        int index = Math.abs((Long.hashCode(key)) % hmaps.length);
        if (hmaps[index] == null && create) {
            synchronized (hmaps) {
                if (hmaps[index] == null) {
                    hmapsSemaphore[index] = new Semaphore(Short.MAX_VALUE, true);
                    hmaps[index] = new LHash<>(new LHMapState<>(capacity, nilKey, skipKey));
                }
            }
        }
        return index;
    }

    public V get(long key) throws InterruptedException {
        int i = hmap(key, false);
        LHash<V> hmap = hmaps[i];
        if (hmap != null) {
            hmapsSemaphore[i].acquire();
            try {
                return hmap.get(key);
            } finally {
                hmapsSemaphore[i].release();
            }
        }
        return null;
    }

    public void remove(long key) throws InterruptedException {
        int i = hmap(key, false);
        LHash<V> hmap = hmaps[i];
        if (hmap != null) {
            hmapsSemaphore[i].acquire(Short.MAX_VALUE);
            try {
                hmap.remove(key);
            } finally {
                hmapsSemaphore[i].release(Short.MAX_VALUE);
            }
        }
    }

    public void clear() throws InterruptedException {
        for (int i = 0; i < hmaps.length; i++) {
            LHash<V> hmap = hmaps[i];
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
        for (LHash<V> hmap : hmaps) {
            if (hmap != null) {
                size += hmap.size();
            }
        }
        return size;
    }

    public boolean stream(LHashValueStream<V> lHashValueStream) throws Exception {
        for (int i = 0; i < hmaps.length; i++) {
            LHash<V> hmap = hmaps[i];
            if (hmap != null) {
                if (!hmap.stream(hmapsSemaphore[i], lHashValueStream)) {
                    return false;
                }
            }
        }
        return true;
    }

}
