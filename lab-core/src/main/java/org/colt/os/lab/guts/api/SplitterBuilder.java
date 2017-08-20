package org.colt.os.lab.guts.api;

import java.util.concurrent.Callable;

/**
 * @author jonathan.colt
 */
public interface SplitterBuilder {

    Callable<Void> buildSplitter(String rawhideName, boolean fsync, SplitterBuilderCallback splitterBuilderCallback) throws Exception;

    interface SplitterBuilderCallback {

        Void call(IndexFactory leftHalfIndexFactory, IndexFactory rightHalfIndexFactory, CommitIndex commitIndex, boolean fsync) throws Exception;
    }

}
