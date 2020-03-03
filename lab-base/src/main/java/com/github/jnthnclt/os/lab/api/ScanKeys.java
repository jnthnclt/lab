package com.github.jnthnclt.os.lab.api;

import com.github.jnthnclt.os.lab.base.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public interface ScanKeys {

    BolBuffer nextKey() throws Exception;
}
