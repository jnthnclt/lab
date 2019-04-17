package com.github.jnthnclt.os.lab.core.guts.api;

import com.github.jnthnclt.os.lab.core.guts.LABFiles;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/**
 * @author jonathan.colt
 */
public interface SplitterBuilder {

    Callable<Void> buildSplitter(String rawhideName,
                                 boolean fsync,
                                 SplitterBuilderCallback callback) throws Exception;

    interface SplitterBuilderCallback {

        Callable<Void> call(IndexFactory leftHalfIndexFactory,
                            IndexFactory rightHalfIndexFactory,
                            CommitIndex commitIndex,
                            boolean fsync,
                            ExecutorService destroy,
                            LABFiles labFiles) throws Exception;
    }

}
