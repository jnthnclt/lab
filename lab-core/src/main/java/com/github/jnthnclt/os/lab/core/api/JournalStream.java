package com.github.jnthnclt.os.lab.core.api;

/**
 *
 * @author jonathan.colt
 */
public interface JournalStream {

    boolean stream(byte[] valueIndexId, byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload) throws Exception;
}
