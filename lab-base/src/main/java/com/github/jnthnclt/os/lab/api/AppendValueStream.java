package com.github.jnthnclt.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface AppendValueStream<P> {

    boolean stream(int index, byte[] key, long timestamp, boolean tombstoned, long version, P payload) throws Exception;
}
