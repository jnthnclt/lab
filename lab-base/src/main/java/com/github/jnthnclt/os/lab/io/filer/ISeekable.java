package com.github.jnthnclt.os.lab.io.filer;

import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public interface ISeekable extends IFilePointer {

    /**
     *
     * @param position
     * @throws java.io.IOException
     */
    public void seek(long position) throws IOException;

}
