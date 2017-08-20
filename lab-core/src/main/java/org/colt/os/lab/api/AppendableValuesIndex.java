package org.colt.os.lab.api;

import java.util.List;
import java.util.concurrent.Future;
import org.colt.os.lab.io.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public interface AppendableValuesIndex<P> {

    boolean append(AppendValues<P> values, boolean fsyncOnFlush, BolBuffer rawEntryBuffer, BolBuffer keyBuffer) throws Exception;

    List<Future<Object>> commit(boolean fsync, boolean waitIfToFarBehind) throws Exception;

    List<Future<Object>> compact(boolean fsync, int minDebt, int maxDebt, boolean waitIfToFarBehind) throws Exception;

    void close(boolean flushUncommited, boolean fsync) throws Exception;

}
