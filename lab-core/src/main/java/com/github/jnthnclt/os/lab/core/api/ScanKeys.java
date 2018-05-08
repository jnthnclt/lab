package com.github.jnthnclt.os.lab.core.api;

import com.github.jnthnclt.os.lab.core.io.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public interface ScanKeys {

    BolBuffer nextKey() throws Exception;
}
