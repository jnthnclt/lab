package com.github.jnthnclt.os.lab.core.api;

import com.github.jnthnclt.os.lab.core.AppendedVersion;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import java.util.List;
import java.util.concurrent.Future;

/**
 * @author jonathan.colt
 */
public interface AppendableValuesIndex<P> {

    AppendedVersion appendedVersion();

    long append(AppendValues<P> values, boolean fsyncOnFlush, BolBuffer rawEntryBuffer, BolBuffer keyBuffer) throws Exception;

    List<Future<Object>> commit(boolean fsync, boolean waitIfToFarBehind) throws Exception;

    List<Future<Object>> compact(boolean fsync, int minDebt, int maxDebt, boolean waitIfToFarBehind) throws Exception;

    void close(boolean flushUncommited, boolean fsync) throws Exception;

    void commitAndWait(long timeoutMillis, boolean fsync) throws Exception;
}
