package org.colt.os.lab.io.api;

import java.io.IOException;
import org.colt.os.lab.io.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public interface IAppendOnly {

    void appendByte(byte b) throws IOException;

    void appendShort(short s) throws IOException;

    void appendInt(int i) throws IOException;

    void appendLong(long l) throws IOException;

    void append(byte b[], int _offset, int _len) throws IOException;

    void append(BolBuffer bolBuffer) throws IOException;

    void flush(boolean fsync) throws IOException;

    void close() throws IOException;

    long length() throws IOException;

    long getFilePointer() throws IOException;

}
