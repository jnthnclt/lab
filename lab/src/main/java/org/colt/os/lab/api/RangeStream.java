package org.colt.os.lab.api;

/**
 * Created by jonathan.colt on 5/19/17.
 */
public interface RangeStream {

    boolean range(int index, byte[] key, byte[] to) throws Exception;
}
