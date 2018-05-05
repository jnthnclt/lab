package com.github.jnthnclt.os.lab.core.guts.api;

import com.github.jnthnclt.os.lab.core.io.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public interface Scanner {

    Next next(RawEntryStream stream, BolBuffer nextHint) throws Exception;

    void close() throws Exception;
}
