package com.github.jnthnclt.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface AppendValues<P> {

    boolean consume(AppendValueStream<P> stream) throws Exception;
}
