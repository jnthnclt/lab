package com.github.jnthnclt.os.lab.core.search;

import com.github.jnthnclt.os.lab.log.LABLogger;
import com.github.jnthnclt.os.lab.log.LABLoggerFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class LABSearchIndexer {

    private static final LABLogger LOG = LABLoggerFactory.getLogger();

    private final int capacity;
    private final LABSearchIndex searchIndex;
    private final AtomicReference<LABSearchIndexUpdates> indexUpdates = new AtomicReference<>(new LABSearchIndexUpdates());
    private final Semaphore semaphore = new Semaphore(Byte.MAX_VALUE);
    private final ExecutorService indexerThread = Executors.newSingleThreadExecutor();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<Exception> failure = new AtomicReference<>(null);

    public LABSearchIndexer(int capacity, LABSearchIndex searchIndex) {
        this.capacity = capacity;
        this.searchIndex = searchIndex;
    }

    public interface Update {
        void update(LABSearchIndexUpdates updates);
    }

    public void update(Update update) throws Exception {
        if (failure.get() != null) {
            throw failure.get();
        }
        semaphore.acquire();
        try {
            LABSearchIndexUpdates updates = indexUpdates.get();
            while (updates.size() >= capacity) {
                synchronized (semaphore) {
                    semaphore.release();
                    semaphore.wait();
                }
                semaphore.acquire();
                updates = indexUpdates.get();
            }
            update.update(updates);
            synchronized (semaphore) {
                semaphore.notifyAll();
            }
        } finally {
            semaphore.release();
        }
    }

    public void reset() {
        failure.set(null);
    }


    public LABSearchIndexUpdates take() throws InterruptedException {
        semaphore.acquire(Byte.MAX_VALUE);
        try {
            LABSearchIndexUpdates pending = indexUpdates.getAndSet(new LABSearchIndexUpdates());
            if (pending.size() == 0) {
                indexUpdates.set(pending);
                return null;
            } else {
                synchronized (semaphore) {
                    semaphore.notifyAll();
                }
            }
            return pending;
        } finally {
            semaphore.release(Byte.MAX_VALUE);
        }
    }


    public void start() {
        if (running.compareAndSet(false, true)) {
            indexerThread.submit(() -> {
                LOG.info("Started indexer...");
                try {
                    while (running.get()) {
                        try {

                            LABSearchIndexUpdates took = take();
                            if (took != null) {
                                try {
                                    LOG.info("Indexing " + took.size());
                                    searchIndex.update(took, false); // TODO add support for deletes
                                    took.clear();
                                } catch (Exception x) {
                                    LOG.error("Indexer has lost updates!", x);
                                    failure.set(x);
                                }
                            } else {
                                synchronized (semaphore) {
                                    semaphore.wait();
                                }
                            }

                        } catch (Exception x) {
                            LOG.error("Indexer failure", x);
                        }
                    }
                } finally {
                    synchronized (running) {
                        running.notifyAll();
                    }
                }
            });
        }
    }

    public void stop() throws InterruptedException {
        synchronized (running) {
            if (running.compareAndSet(true, false)) {
                running.set(false);
                LOG.info("Waiting for indexer to complete...");
                running.wait();
                LOG.info("Indexer stop.");
            }
        }
    }


}
