package org.colt.os.lab.bitmaps;

/**
 * Created by jonathan.colt on 6/28/17.
 */
public interface LABIndexTx<R, IBM> {

    R tx(IBM bitmap) throws Exception;

}
