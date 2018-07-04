package com.github.jnthnclt.os.lab.core.guts.api;

import com.github.jnthnclt.os.lab.base.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public interface Scanner {

    BolBuffer next(BolBuffer rawEntry, BolBuffer nextHint) throws Exception;

    void close() throws Exception;
}
