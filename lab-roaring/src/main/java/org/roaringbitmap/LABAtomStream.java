package org.roaringbitmap;

import java.io.DataInput;
import java.io.IOException;

public interface LABAtomStream {
    boolean stream(int key, DataInput dataInput) throws IOException;
}
