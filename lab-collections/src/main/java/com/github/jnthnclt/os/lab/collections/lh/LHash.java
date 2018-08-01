package com.github.jnthnclt.os.lab.collections.lh;

import java.util.concurrent.Semaphore;

/**
 *
 * @author jonathan.colt
 */
public class LHash<V> {

    private volatile LHMapState< V> state;

    public LHash(LHMapState<V> state) {
        this.state = state;
    }

    public int size() {
        return state.size();
    }

    public void clear() {
        state = state.allocate(0);
    }

    public V get(long key) {
        return get(Long.hashCode(key), key);
    }

    public V get(int hashCode, long key) {
        LHMapState< V> s = state;
        long nil = s.nil();
        long skipped = s.skipped();
        if (key == nil || key == skipped) {
            return null;
        }
        if (s.size() == 0) {
            return null;
        }
        int capacity = s.capacity();
        int start = s.indexForHash(hashCode);
        for (int i = start, j = 0;  // stack vars for efficiency
             j < capacity; // max search for key
             i = (++i) % capacity, j++) { // wraps around table

            long storedKey = s.key(i);
            if (storedKey == skipped) {
                continue;
            }
            if (storedKey == nil) {
                return null;
            }
            if (storedKey == key) {
                return s.value(i);
            }
        }
        return null;
    }

    public void remove(long key) {
        remove(Long.hashCode(key), key);
    }

    @SuppressWarnings("unchecked")
    public void remove(int hashCode, long key) {
        LHMapState<V> s = state;
        long nil = s.nil();
        long skipped = s.skipped();
        if (key == nil || key == skipped) {
            return;
        }
        if (s.size() == 0) {
            return;
        }
        int capacity = s.capacity();
        int start = s.indexForHash(hashCode);
        for (int i = start, j = 0;  // stack vars for efficiency
             j < capacity; // max search for key
             i = (++i) % capacity, j++) { // wraps around table

            long storedKey = s.key(i);
            if (storedKey == skipped) {
                continue;
            }
            if (storedKey == nil) {
                return;
            }

            if (storedKey == key) {
                int next = (i + 1) % capacity;

                s.remove(i, skipped, null);
                if (s.key(next) == nil) {
                    for (int z = i, y = 0; y < capacity; z = (z + capacity - 1) % capacity, y++) {
                        if (s.key(z) != skipped) {
                            break;
                        }
                        s.clear(z);
                    }
                }
                return;
            }
        }
    }

    public void put(long key, V value) {
        put(Long.hashCode(key), key, value);
    }

    @SuppressWarnings("unchecked")
    public void put(int hashCode, long key, V value) {
        LHMapState<V> s = state;
        long capacity = s.capacity();
        if (s.size() * 2 >= capacity) {
            LHMapState<V> to = s.allocate(s.capacity() * 2);
            rehash(s, to);
            state = to;
            s = to;
        }
        internalPut(s, hashCode, key, value);
    }

    private void internalPut(LHMapState<V> s, int hashCode, long key, V value) {
        int capacity = s.capacity();
        int start = s.indexForHash(hashCode);
        long nil = s.nil();
        long skipped = s.skipped();
        for (int i = start, j = 0;  // stack vars for efficiency
             j < capacity; // max search for available slot
             i = (++i) % capacity, j++) { // wraps around table

            long storedKey = s.key(i);
            if (storedKey == nil || storedKey == skipped) {
                s.link(i, key, value);
                return;
            }
            if (storedKey == key) {
                s.update(i, key, value);
                return;
            }
        }
    }

    private void rehash(LHMapState<V> from, LHMapState<V> to) {
        int i = from.first();
        long nil = to.nil();
        long skipped = to.skipped();
        while (i != -1) {
            long storedKey = from.key(i);
            if (storedKey != nil && storedKey != skipped) {
                int hash = Long.hashCode(storedKey);
                internalPut(to, hash, storedKey, from.value(i));
            }
            i = from.next(i);
        }
    }

//    private long hash(LHMapState state, long keyShuffle) {
//        keyShuffle += keyShuffle >> 8; // shuffle bits to avoid worst case clustering
//
//        if (keyShuffle < 0) {
//            keyShuffle = -keyShuffle;
//        }
//        return keyShuffle % state.capacity();
//    }

    public boolean stream(Semaphore semaphore, LHashValueStream<V> stream) throws Exception {
        LHMapState<V> s = state;
        int c = s.capacity();
        if (c <= 0) {
            return true;
        }
        long nil = s.nil();
        long skipped = s.skipped();
        int i = s.first();
        while (i != -1) {

            long key;
            V value = null;
            semaphore.acquire();
            try {
                key = s.key(i);
                if (key != nil && key != skipped) {
                    value = s.value(i);
                }
            } finally {
                semaphore.release();
            }
            if (key != nil && key != skipped) {
                if (!stream.keyValue(key, value)) {
                    return false;
                }
            }
            i = s.next(i);
        }
        return true;
    }

}
