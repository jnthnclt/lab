package com.github.jnthnclt.os.lab.core.guts;

import java.io.File;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

public class LABFiles {

    private final Semaphore semaphore = new Semaphore(Short.MAX_VALUE);

    private final AtomicReference<Queue<AppendedFile>> appendedFiles = new AtomicReference<>(new ConcurrentLinkedQueue<>());
    private final AtomicReference<Queue<File>> removedFiles = new AtomicReference<>(new ConcurrentLinkedQueue<>());

    public void add(byte[] labId, long appendedVersion, File file) throws InterruptedException {
        semaphore.acquire(1);
        try {
            appendedFiles.get().add(new AppendedFile(labId, appendedVersion, file));
        } finally {
            semaphore.release(1);
        }
    }

    public static class AppendedFile {
        public final byte[] labId;
        public final long appendVersion;
        public final File file;

        public AppendedFile(byte[] labId, long appendVersion, File file) {
            this.labId = labId;
            this.appendVersion = appendVersion;
            this.file = file;
        }

        @Override
        public String toString() {
            return "AppendedFile{" +
                "labId=" + Arrays.toString(labId) +
                ", appendVersion=" + appendVersion +
                ", file=" + file +
                '}';
        }
    }

    public void delete(File file) throws InterruptedException {
        semaphore.acquire(1);
        try {
            removedFiles.get().add(file);
        } finally {
            semaphore.release(1);
        }
    }

    public interface LABFileChanges {
        boolean took(Queue<AppendedFile> appendedFiles, Queue<File> removedFiles) throws Exception;
    }

    public void take(LABFileChanges labFileChanges) throws Exception {
        Queue<AppendedFile> appended;
        Queue<File> removed;
        semaphore.acquire(Short.MAX_VALUE);
        try {
            appended = appendedFiles.getAndSet(new ConcurrentLinkedQueue<>());
            removed = removedFiles.getAndSet(new ConcurrentLinkedQueue<>());
        } finally {
            semaphore.release(Short.MAX_VALUE);
        }
        labFileChanges.took(appended, removed);
    }

    public boolean isEmpty() {
        return appendedFiles.get().isEmpty() || removedFiles.get().isEmpty();
    }

    public static void main(String[] args) throws Exception {


        LABFiles labFiles = new LABFiles();
        labFiles.take((appendedFiles1, removedFiles1) -> {
            for (AppendedFile appendedFile : appendedFiles1) {
                System.out.println(appendedFile.file);
            }

            for (File removed : removedFiles1) {
                System.out.println(removed);
            }

            return true;
        });
    }
}
