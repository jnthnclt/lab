package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;

/**
 * @author jonathan.colt
 */
public interface LABMapProvider<E, B> {

    LABMap<E, B> create(Rawhide rawhide, int poweredUpToHint) throws Exception;
}
