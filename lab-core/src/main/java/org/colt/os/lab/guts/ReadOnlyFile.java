package org.colt.os.lab.guts;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicBoolean;
import org.colt.os.lab.io.PointerReadableByteBufferFile;

/**
 * @author jonathan.colt
 */
public class ReadOnlyFile {

    public static final long BUFFER_SEGMENT_SIZE = 1024L * 1024 * 1024;

    private final File file;
    private final RandomAccessFile randomAccessFile;
    private final long size;

    private volatile PointerReadableByteBufferFile pointerReadable;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ReadOnlyFile(File file) throws IOException {
        this.file = file;
        this.randomAccessFile = new RandomAccessFile(file, "r");
        this.size = randomAccessFile.length();
    }

    public String getFileName() {
        return file.toString();
    }

    public void delete() {
        file.delete();
    }

    public boolean isClosed() {
        return closed.get();
    }

    public PointerReadableByteBufferFile pointerReadable(long bufferSegmentSize) throws IOException {
        if (pointerReadable == null) {
            pointerReadable = new PointerReadableByteBufferFile(bufferSegmentSize > 0 ? bufferSegmentSize : BUFFER_SEGMENT_SIZE, file, false);
        }
        return pointerReadable;
    }

    public void fsync() throws IOException {
        randomAccessFile.getFD().sync();
    }

    @Override
    public String toString() {
        return "ReadOnlyFile{"
            + "fileName=" + file
            + ", size=" + size
            + '}';
    }

    public void close() throws IOException {
        synchronized (closed) {
            if (closed.compareAndSet(false, true)) {
                randomAccessFile.close();
                if (pointerReadable != null) {
                    pointerReadable.close();
                }
            }
        }
    }

    public long length() throws IOException {
        return size;
    }
}
