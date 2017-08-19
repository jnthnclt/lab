package org.colt.os.lab.guts;

import org.colt.os.lab.api.rawhide.Rawhide;

/**
 * @author jonathan.colt
 */
public interface LABIndexProvider<E, B> {

    LABIndex<E, B> create(Rawhide rawhide, int poweredUpToHint) throws Exception;
}
