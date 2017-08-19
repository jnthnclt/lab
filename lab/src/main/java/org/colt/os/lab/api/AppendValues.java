package org.colt.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface AppendValues<P> {

    boolean consume(AppendValueStream<P> stream) throws Exception;
}
