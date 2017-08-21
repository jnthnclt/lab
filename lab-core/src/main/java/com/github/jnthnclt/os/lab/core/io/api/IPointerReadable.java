package com.github.jnthnclt.os.lab.core.io.api;

import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import java.io.IOException;

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
