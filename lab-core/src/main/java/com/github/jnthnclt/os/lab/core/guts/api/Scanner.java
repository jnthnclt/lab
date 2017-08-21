package com.github.jnthnclt.os.lab.core.guts.api;

/**
 *
 * @author jonathan.colt
 */
public interface Scanner {

    Next next(RawEntryStream stream) throws Exception;

    void close() throws Exception;
}
