package com.github.jnthnclt.os.lab.core.guts.api;

import java.util.concurrent.Callable;

/**
 * @author jonathan.colt
 */
public interface SplitterBuilder {

    Callable<Void> buildSplitter(String rawhideName, boolean fsync, SplitterBuilderCallback callback) throws Exception;

    interface SplitterBuilderCallback {

        Callable<Void> call(IndexFactory leftHalfIndexFactory, IndexFactory rightHalfIndexFactory, CommitIndex commitIndex, boolean fsync) throws Exception;
    }

}
