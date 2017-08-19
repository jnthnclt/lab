package org.colt.os.lab.bitmaps;

import javax.annotation.processing.Filer;

/**
 * Created by jonathan.colt on 6/28/17.
 */
public interface LABMultiBitmapIndexTx<IBM> {
    void tx(int index, int lastId, IBM bitmap, Filer filer, int offset) throws Exception;
}
