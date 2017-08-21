package com.github.jnthnclt.os.lab.collections;

/**
 *
 * @author jonathan.colt
 */
public interface KeyValueStream<K, V> {

    boolean keyValue(K key, V value) throws Exception;

}
