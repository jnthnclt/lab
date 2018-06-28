package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.core.guts.api.ReadIndex;

/**
 *
 * @author jonathan.colt
 */
public interface ReaderTx {

    boolean tx(int index, boolean pointFrom, byte[] fromKey, byte[] toKey, ReadIndex[] readIndexs, boolean hydrateValues) throws Exception;

}
