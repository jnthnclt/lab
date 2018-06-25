package com.github.jnthnclt.os.lab.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import com.github.jnthnclt.os.lab.core.api.exceptions.LABClosedException;
import com.github.jnthnclt.os.lab.core.api.exceptions.LABCorruptedException;
import com.github.jnthnclt.os.lab.core.util.LABLogger;
import com.github.jnthnclt.os.lab.core.util.LABLoggerFactory;

/**
 * @author jonathan.colt
 */
public class LABHeapPressure {

    public enum FreeHeapStrategy {
        mostBytesFirst, oldestAppendFirst, longestElapseSinceCommit
    }

    private static final LABLogger LOG = LABLoggerFactory.getLogger();

    private final LABStats stats;
    private final ExecutorService schedule;
    private final String name;
    private final long maxHeapPressureInBytes;
    private final long blockOnHeapPressureInBytes;
    private final AtomicLong globalHeapCostInBytes;
    private final Map<LAB, Boolean> committableLabs = Maps.newConcurrentMap();
    private volatile boolean running = false;
    private final AtomicLong changed = new AtomicLong();
    private final AtomicLong waiting = new AtomicLong();
    private final FreeHeapStrategy freeHeapStrategy;

    public LABHeapPressure(LABStats stats,
        ExecutorService schedule,
        String name,
        long maxHeapPressureInBytes,
        long blockOnHeapPressureInBytes,
        AtomicLong globalHeapCostInBytes,
        FreeHeapStrategy freeHeapStrategy
    ) {

        this.stats = stats;
        this.schedule = schedule;
        this.name = name;
        this.maxHeapPressureInBytes = maxHeapPressureInBytes;
        this.blockOnHeapPressureInBytes = blockOnHeapPressureInBytes;
        this.globalHeapCostInBytes = globalHeapCostInBytes;
        this.freeHeapStrategy = freeHeapStrategy;

        Preconditions.checkArgument(maxHeapPressureInBytes <= blockOnHeapPressureInBytes,
            "maxHeapPressureInBytes must be less than or equal to blockOnHeapPressureInBytes");
    }

    public void change(long delta) {
        changed.incrementAndGet();
        globalHeapCostInBytes.addAndGet(delta);
        if (delta < 0) {
            stats.freed.add(-delta);
            synchronized (globalHeapCostInBytes) {
                globalHeapCostInBytes.notifyAll();
            }
        } else {
            stats.slabbed.add(delta);
        }
    }

    void commitIfNecessary(LAB lab, long labMaxHeapPressureInBytes, boolean fsyncOnFlush) throws Exception {
        if (lab.approximateHeapPressureInBytes() > labMaxHeapPressureInBytes) {
            lab.commit(fsyncOnFlush, false); // todo config
            committableLabs.remove(lab);
            stats.pressureCommit.increment();
        } else {
            committableLabs.compute(lab, (t, u) -> u == null ? fsyncOnFlush : u || fsyncOnFlush);
        }

        long globalHeap = globalHeapCostInBytes.get();
        stats.commitable.set(committableLabs.size());
        if (globalHeap > maxHeapPressureInBytes) {

            synchronized (globalHeapCostInBytes) {
                waiting.incrementAndGet();
            }
            try {
                boolean loggedBlocking = false;
                boolean loggedSlow = false;
                while (globalHeap > blockOnHeapPressureInBytes) {
                    long version = changed.get();
                    freeHeap();
                    synchronized (globalHeapCostInBytes) {
                        if (version == changed.get()) {
                            long start = System.currentTimeMillis();
                            if (!loggedBlocking) {
                                LOG.warn(" {} BLOCKING LAB writes. Waiting on heap to go down...{} > {}", lab.name(), globalHeap, blockOnHeapPressureInBytes);
                                loggedBlocking = true;
                            }
                            globalHeapCostInBytes.wait(1_000);
                            if (!loggedSlow) {
                                if (System.currentTimeMillis() - start > 1_000) {
                                    LOG.warn("{} Still BLOCKING. Taking more than 1 sec to free heap. Nudging freeHeap!", lab.name());
                                    loggedSlow = true;
                                }
                            }
                        }
                    }
                    globalHeap = globalHeapCostInBytes.get();
                }
            } finally {
                waiting.decrementAndGet();
            }
        }
    }

    private static class Freeable {

        private final LAB lab;
        private final long approximateHeapPressureInBytes;
        private final long lastAppendTimestamp;
        private final long lastCommitTimestamp;

        Freeable(LAB lab, long approximateHeapPressureInBytes, long lastAppendTimestamp, long lastCommitTimestamp) {
            this.lab = lab;
            this.approximateHeapPressureInBytes = approximateHeapPressureInBytes;
            this.lastAppendTimestamp = lastAppendTimestamp;
            this.lastCommitTimestamp = lastCommitTimestamp;
        }

    }

    public void freeHeap() {
        synchronized (globalHeapCostInBytes) {
            if (!running) {
                long globalHeap = globalHeapCostInBytes.get();
                if (globalHeap > maxHeapPressureInBytes) {
                    running = true;
                    long greed = (long)(maxHeapPressureInBytes * 0.50); // TODO config?
                    AtomicLong debtInBytes = new AtomicLong(globalHeap - greed);
                    schedule.submit(() -> {

                        stats.gc.increment();
                        try {
                            LAB[] labs = committableLabs.keySet().toArray(new LAB[0]);
                            if (labs.length > 0) {

                                Freeable[] freeables = new Freeable[labs.length];
                                for (int i = 0; i < labs.length; i++) {
                                    freeables[i] = new Freeable(labs[i],
                                        labs[i].approximateHeapPressureInBytes(),
                                        labs[i].lastAppendTimestamp(),
                                        labs[i].lastCommitTimestamp()
                                    );
                                }
                                if (freeHeapStrategy == FreeHeapStrategy.mostBytesFirst) {
                                    Arrays.sort(freeables, (o1, o2) -> -Long.compare(o1.approximateHeapPressureInBytes, o2.approximateHeapPressureInBytes));
                                } else if (freeHeapStrategy == FreeHeapStrategy.oldestAppendFirst) {
                                    Arrays.sort(freeables, Comparator.comparingLong(o -> o.lastAppendTimestamp));
                                } else if (freeHeapStrategy == FreeHeapStrategy.longestElapseSinceCommit) {
                                    Arrays.sort(freeables, Comparator.comparingLong(o -> o.lastCommitTimestamp));
                                }

                                List<Future<Object>> waitForCommits = Lists.newArrayList();
                                for (Freeable freeable : freeables) {
                                    if (debtInBytes.get() <= 0) {
                                        break;
                                    }
                                    LOG.debug("Freeing {} for {}", freeable.approximateHeapPressureInBytes, freeable.lab.name());
                                    Boolean efsyncOnFlush = this.committableLabs.remove(freeable.lab);
                                    if (efsyncOnFlush != null) {
                                        try {
                                            waitForCommits.addAll(freeable.lab.commit(efsyncOnFlush, false));
                                            stats.gcCommit.increment();
                                        } catch (LABCorruptedException | LABClosedException x) {
                                            LOG.error("Failed to commit.", x);
                                        } catch (Exception x) {
                                            this.committableLabs.compute(freeable.lab,
                                                (t, u) -> u == null ? efsyncOnFlush : (boolean) u || efsyncOnFlush);
                                            throw x;
                                        }
                                    }
                                    debtInBytes.addAndGet(-freeable.approximateHeapPressureInBytes);
                                }

                                for (Future<Object> c : waitForCommits) {
                                    c.get();
                                }
                            }
                            return true;
                        } catch (InterruptedException ie) {
                            throw ie;
                        } catch (Exception x) {
                            LOG.warn("Free heap encountered an error.", x);
                            return false;
                        } finally {
                            synchronized (globalHeapCostInBytes) {
                                running = false;
                            }
                        }
                    });
                }
            }
        }
    }

    void close(LAB lab) {
        committableLabs.remove(lab);
    }

}
