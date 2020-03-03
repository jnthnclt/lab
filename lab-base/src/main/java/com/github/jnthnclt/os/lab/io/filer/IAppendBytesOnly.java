package com.github.jnthnclt.os.lab.io.filer;

import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public interface IAppendBytesOnly extends ICloseable, IFilePointer {

    void write(byte b) throws IOException;

    /**
     *
     * @param b
     * @param _offset
     * @param _len
     * @throws java.io.IOException
     */
     void write(byte b[], int _offset, int _len) throws IOException;

    /**
     *
     * @throws java.io.IOException
     */
     void flush(boolean fsync) throws IOException;
}