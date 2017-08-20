package org.colt.os.lab.guts.allocators;

/**
 *
 * @author jonathan.colt
 */
public interface LABCostChangeInBytes {

    void cost(long allocated, long reused);
}
