package com.github.jnthnclt.os.lab.core.guts.api;

import com.github.jnthnclt.os.lab.core.guts.LABFiles;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/**
 * @author jonathan.colt
 */
public interface MergerBuilder {

    Callable<Void> build(String rawhideName,
                         int minimumRun,
                         boolean fsync,
                         MergerBuilderCallback callback) throws Exception;

    interface MergerBuilderCallback {

        Callable<Void> call(int minimumRun,
                            boolean fsync,
                            IndexFactory indexFactory,
                            CommitIndex commitIndex,
                            ExecutorService destroy,
                            LABFiles labFiles) throws Exception;
    }

}
