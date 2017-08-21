package com.github.jnthnclt.os.lab.core.guts.api;

import com.github.jnthnclt.os.lab.core.guts.IndexRangeId;
import java.util.List;
import com.github.jnthnclt.os.lab.core.guts.ReadOnlyIndex;

/**
 *
 * @author jonathan.colt
 */
public interface CommitIndex {

    ReadOnlyIndex commit(List<IndexRangeId> ids) throws Exception;

}
