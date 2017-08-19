package org.colt.os.lab.guts.api;

import org.colt.os.lab.guts.ActiveScan;
import org.colt.os.lab.io.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public interface ReadIndex {

    void release();

    Scanner rangeScan(ActiveScan activeScan, byte[] from, byte[] to, BolBuffer entryBuffer, BolBuffer entryKeyBuffer) throws Exception;

    Scanner rowScan(ActiveScan activeScan, BolBuffer entryBuffer, BolBuffer entryKeyBuffer) throws Exception;

    Scanner pointScan(ActiveScan activeScan, byte[] key, BolBuffer entryBuffer, BolBuffer entryKeyBuffer) throws Exception;

    long count() throws Exception;

}
