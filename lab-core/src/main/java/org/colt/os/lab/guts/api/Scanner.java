package org.colt.os.lab.guts.api;

/**
 *
 * @author jonathan.colt
 */
public interface Scanner {

    Next next(RawEntryStream stream) throws Exception;

    void close() throws Exception;
}
