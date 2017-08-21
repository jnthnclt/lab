package com.github.jnthnclt.os.lab.core.guts.allocators;

/**
 *
 * @author jonathan.colt
 */
public interface LABCostChangeInBytes {

    void cost(long allocated, long reused);
}
