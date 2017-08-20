package org.colt.os.lab.guts.api;

import org.colt.os.lab.io.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public interface RawAppendableIndex {

    boolean append(AppendEntries entries, BolBuffer keyBuffer) throws Exception;

    void closeAppendable(boolean fsync) throws Exception;
}
