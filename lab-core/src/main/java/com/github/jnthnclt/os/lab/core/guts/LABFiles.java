package com.github.jnthnclt.os.lab.core.guts;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

public class LABFiles {

    private final ConcurrentLinkedQueue<AppendedFile> changes = new ConcurrentLinkedQueue<>();
    private final AtomicLong version = new AtomicLong();

    public void add(byte[] labId,
                    long fromAppendVersion,
                    long toAppendVersion,
                    File file) {
        synchronized (version) {
            changes.add(new AppendedFile(labId, fromAppendVersion, toAppendVersion, file, false));
            version.incrementAndGet();
            version.notifyAll();
        }
    }

    public void delete(byte[] labId, File file) {
        synchronized (version) {
            changes.add(new AppendedFile(labId, -1,-1, file, true));
            version.incrementAndGet();
            version.notifyAll();
        }
    }

    public interface LABFileChanges {
        boolean took(AppendedFile change) throws Exception;
    }

    public void take(LABFileChanges labFileChanges) throws Exception {
        while (true) {
            long v = version.get();
            AppendedFile change = changes.poll();
            if (!labFileChanges.took(change)) {
                if (change != null) {
                    changes.add(change);
                }
                return;
            }
            if (change == null) {
                synchronized (version) {
                    if (v == version.get()) {
                        version.wait(1000);
                    }
                }
            }
        }
    }


    public static class AppendedFile {
        public final byte[] labId;
        public final long fromAppendVersion;
        public final long toAppendVersion;
        public final File file;
        public final boolean delete;

        public AppendedFile(byte[] labId,
                            long fromAppendVersion,
                            long toAppendVersion,
                            File file,
                            boolean delete) {
            this.labId = labId;
            this.fromAppendVersion = fromAppendVersion;
            this.toAppendVersion = toAppendVersion;
            this.file = file;
            this.delete = delete;
        }

        @Override
        public String toString() {
            return "AppendedFile{" +
                    "labId=" + Arrays.toString(labId) +
                    ", fromAppendVersion=" + fromAppendVersion +
                    ", toAppendVersion=" + toAppendVersion +
                    ", file=" + file +
                    ", delete=" + delete +
                    '}';
        }
    }
}
