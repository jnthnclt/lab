package com.github.jnthnclt.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface ValueIndex<P> extends ReadValueIndex, AppendableValuesIndex<P> {

    int debt() throws Exception;

    boolean closed();

}
