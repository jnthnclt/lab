package com.github.jnthnclt.os.lab.collections.lh;

/**
 *
 * @author jonathan.colt
 */
public interface LHashValueStream<V> {

    boolean keyValue(long key, V value) throws Exception;

}
