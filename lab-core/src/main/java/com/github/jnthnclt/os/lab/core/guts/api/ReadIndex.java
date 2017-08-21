package com.github.jnthnclt.os.lab.core.guts.api;

import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import com.github.jnthnclt.os.lab.core.guts.ActiveScan;

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
