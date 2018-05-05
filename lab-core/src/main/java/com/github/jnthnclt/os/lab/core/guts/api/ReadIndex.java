package com.github.jnthnclt.os.lab.core.guts.api;

import com.github.jnthnclt.os.lab.core.guts.ActiveScanPoint;
import com.github.jnthnclt.os.lab.core.guts.ActiveScanRange;
import com.github.jnthnclt.os.lab.core.guts.ActiveScanRow;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public interface ReadIndex {

    void release();

    Scanner rangeScan(ActiveScanRange activeScan, byte[] from, byte[] to, BolBuffer entryBuffer, BolBuffer entryKeyBuffer) throws Exception;

    Scanner rowScan(ActiveScanRow activeScan, BolBuffer entryBuffer, BolBuffer entryKeyBuffer) throws Exception;

    Scanner pointScan(ActiveScanPoint activeScan, byte[] key, BolBuffer entryBuffer, BolBuffer entryKeyBuffer) throws Exception;

    long count() throws Exception;

}
