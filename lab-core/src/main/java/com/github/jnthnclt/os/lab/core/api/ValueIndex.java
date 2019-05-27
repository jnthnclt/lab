package com.github.jnthnclt.os.lab.core.api;

/**
 *
 * @author jonathan.colt
 */
public interface ValueIndex<P> extends ReadValueIndex, AppendableValuesIndex<P> {

    int debt() throws Exception;

    boolean closed();

}
