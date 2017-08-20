package org.colt.os.lab.guts.api;

import java.util.concurrent.Callable;

/**
 * @author jonathan.colt
 */
public interface MergerBuilder {

    Callable<Void> build(String rawhideName, int minimumRun, boolean fsync, MergerBuilderCallback callback) throws Exception;

    interface MergerBuilderCallback {

        Callable<Void> call(int minimumRun, boolean fsync, IndexFactory indexFactory, CommitIndex commitIndex) throws Exception;
    }

}
