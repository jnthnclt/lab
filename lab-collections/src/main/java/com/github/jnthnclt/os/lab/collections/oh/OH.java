package com.github.jnthnclt.os.lab.collections.oh;

import com.github.jnthnclt.os.lab.collections.KeyValueStream;
import java.util.concurrent.Semaphore;

/**
 *
 * @author jonathan.colt
 */
public interface OH<K, V> {

    void clear();

    V get(K key);

    V get(long hashCode, K key);

    void put(K key, V value);

    @SuppressWarnings(value = "unchecked")
    void put(long hashCode, K key, V value);

    void remove(K key);

    @SuppressWarnings(value = "unchecked")
    void remove(long hashCode, K key);

    long size();

    boolean stream(Semaphore semaphore, KeyValueStream<K, V> stream) throws Exception;

}
