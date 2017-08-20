package org.colt.os.lab.guts.api;

import java.util.List;
import org.colt.os.lab.guts.IndexRangeId;
import org.colt.os.lab.guts.ReadOnlyIndex;

/**
 *
 * @author jonathan.colt
 */
public interface CommitIndex {

    ReadOnlyIndex commit(List<IndexRangeId> ids) throws Exception;

}
