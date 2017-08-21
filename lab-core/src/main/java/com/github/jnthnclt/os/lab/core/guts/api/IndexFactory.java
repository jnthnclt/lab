package com.github.jnthnclt.os.lab.core.guts.api;

import com.github.jnthnclt.os.lab.core.guts.IndexRangeId;
import com.github.jnthnclt.os.lab.core.guts.LABAppendableIndex;

/**
 *
 * @author jonathan.colt
 */
public interface IndexFactory {

    LABAppendableIndex createIndex(IndexRangeId id, long worstCaseCount) throws Exception;
}
