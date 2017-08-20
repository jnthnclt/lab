package org.colt.os.lab.api;

import org.colt.os.lab.io.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public interface ValueStream {

    boolean stream(int index, BolBuffer key, long timestamp, boolean tombstoned, long version, BolBuffer payload) throws Exception;
}
