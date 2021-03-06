package com.github.jnthnclt.os.lab.api;

import com.github.jnthnclt.os.lab.base.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public interface ValueStream {

    boolean stream(int index, BolBuffer key, long timestamp, boolean tombstoned, long version, BolBuffer payload) throws Exception;
}
