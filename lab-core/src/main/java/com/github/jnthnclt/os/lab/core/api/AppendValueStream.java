package com.github.jnthnclt.os.lab.core.api;

/**
 *
 * @author jonathan.colt
 */
public interface AppendValueStream<P> {

    boolean stream(int index, byte[] key, long timestamp, boolean tombstoned, long version, P payload) throws Exception;
}
