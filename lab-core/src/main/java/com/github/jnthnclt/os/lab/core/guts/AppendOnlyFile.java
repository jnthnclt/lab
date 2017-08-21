package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.core.api.exceptions.LABClosedException;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import com.github.jnthnclt.os.lab.core.io.api.IAppendOnly;

/**
 * @author jonathan.colt
 */
public class AppendOnlyFile {

    private final File file;
    private RandomAccessFile randomAccessFile;
    private final AtomicLong size;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public AppendOnlyFile(File file) throws IOException {
        this.file = file;
        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.size = new AtomicLong(randomAccessFile.length());
    }

    public File getFile() {
        return file;
    }

    public void delete() {
        file.delete();
    }

    public void flush(boolean fsync) throws IOException {
        if (fsync) {
            randomAccessFile.getFD().sync();
        }
    }

    public IAppendOnly appender() throws Exception {
        if (closed.get()) {
            throw new LABClosedException("Cannot get an appender from an index that is already closed.");
        }
        DataOutputStream writer = new DataOutputStream(new FileOutputStream(file, true));
        return new IAppendOnly() {
            @Override
            public void appendByte(byte b) throws IOException {
                writer.writeByte(b);
                size.addAndGet(1);
            }

            @Override
            public void appendShort(short s) throws IOException {
                writer.writeShort(s);
                size.addAndGet(2);
            }

            @Override
            public void appendInt(int i) throws IOException {
                writer.writeInt(i);
                size.addAndGet(4);
            }

            @Override
            public void appendLong(long l) throws IOException {
                writer.writeLong(l);
                size.addAndGet(8);
            }

            @Override
            public void append(byte[] b, int _offset, int _len) throws IOException {
                writer.write(b, _offset, _len);
                size.addAndGet(_len);
            }

            @Override
            public void append(BolBuffer bolBuffer) throws IOException {
                byte[] copy = bolBuffer.copy();
                append(copy, 0, copy.length);
            }

            @Override
            public void flush(boolean fsync) throws IOException {
                writer.flush();
                AppendOnlyFile.this.flush(fsync);
            }

            @Override
            public void close() throws IOException {
                writer.close();
            }

            @Override
            public long length() throws IOException {
                return AppendOnlyFile.this.length();
            }

            @Override
            public long getFilePointer() throws IOException {
                return length();
            }

        };
    }

    public void close() throws IOException {
        synchronized (closed) {
            if (closed.compareAndSet(false, true)) {
                randomAccessFile.close();
            }
        }
    }

    public long length() {
        return size.get();
    }

    @Override
    public String toString() {
        return "IndexFile{"
            + "fileName=" + file
            + ", size=" + size
            + '}';
    }

}
