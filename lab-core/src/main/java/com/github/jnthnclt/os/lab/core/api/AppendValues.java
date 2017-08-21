package com.github.jnthnclt.os.lab.core.api;

/**
 *
 * @author jonathan.colt
 */
public interface AppendValues<P> {

    boolean consume(AppendValueStream<P> stream) throws Exception;
}
