package com.github.jnthnclt.os.lab.core.guts.api;

import com.github.jnthnclt.os.lab.base.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public interface RawAppendableIndex {

    boolean append(AppendEntries entries, BolBuffer keyBuffer) throws Exception;

    void closeAppendable(boolean fsync) throws Exception;
}
