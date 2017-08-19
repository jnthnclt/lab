package org.colt.os.lab.guts.api;

import org.colt.os.lab.guts.IndexRangeId;
import org.colt.os.lab.guts.LABAppendableIndex;

/**
 *
 * @author jonathan.colt
 */
public interface IndexFactory {

    LABAppendableIndex createIndex(IndexRangeId id, long worstCaseCount) throws Exception;
}
