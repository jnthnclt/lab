package org.colt.os.lab.io.api;

import java.io.IOException;
import org.colt.os.lab.io.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public interface IPointerReadable {

    long length();

    int read(long readPointer) throws IOException;

    int readInt(long readPointer) throws IOException;

    long readLong(long readPointer) throws IOException;

    int read(long readPointer, byte b[], int _offset, int _len) throws IOException;

    BolBuffer sliceIntoBuffer(long offset, int length, BolBuffer entryBuffer) throws IOException;

}
