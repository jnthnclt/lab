package com.github.jnthnclt.os.lab.core;

import com.google.common.base.Preconditions;
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
            LOG.inc("lab>pressure>commit>" + name);
            committableLabs.remove(lab);
            lab.commit(fsyncOnFlush, false); // todo config
            stats.pressureCommit.increment();
        } else {
            committableLabs.compute(lab, (t, u) -> u == null ? fsyncOnFlush : u || fsyncOnFlush);
        }
        long globalHeap = globalHeapCostInBytes.get();
        LOG.set("lab>heap>pressure>" + name, globalHeap);
        LOG.set("lab>committable>" + name, committableLabs.size());
        if (globalHeap > maxHeapPressureInBytes) {

            synchronized (globalHeapCostInBytes) {
                waiting.incrementAndGet();
            }
            try {
                long version = changed.get();
                freeHeap();
                boolean nudgeFreeHeap = false;
                while (globalHeap > blockOnHeapPressureInBytes) {
                    LOG.debug("BLOCKING for heap to go down...{} > {}", globalHeap, blockOnHeapPressureInBytes);
                    try {
                        LOG.incAtomic("lab>heap>blocking>" + name);
                        synchronized (globalHeapCostInBytes) {
                            long got = changed.get();
                            if (version == got) {
                                long start = System.currentTimeMillis();
                                globalHeapCostInBytes.wait(60_000);
                                if (System.currentTimeMillis() - start > 60_000) {
                                    LOG.warn("Taking more than 60sec to free heap.");
                                    nudgeFreeHeap = true;
                                }
                            } else {
                                version = got;
                                nudgeFreeHeap = true;
                            }
                        }
                        if (nudgeFreeHeap) {
                            nudgeFreeHeap = false;
                            LOG.info("Nudging freeHeap()  {} > {}", globalHeap, blockOnHeapPressureInBytes);
                            version = changed.get();
                            freeHeap();
                            Thread.yield();
                        }

                        globalHeap = globalHeapCostInBytes.get();
                    } finally {
                        LOG.decAtomic("lab>heap>blocking>" + name);
                    }
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
                running = true;
                schedule.submit(() -> {
                    stats.gc.increment();
                    try {
                        long debtInBytes = globalHeapCostInBytes.get() - maxHeapPressureInBytes;
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

                            for (int i = 0; i < freeables.length && debtInBytes > 0; i++) {
                                debtInBytes -= freeables[i].approximateHeapPressureInBytes;
                                Boolean efsyncOnFlush = this.committableLabs.remove(freeables[i].lab);
                                if (efsyncOnFlush != null) {
                                    try {
                                        List<Future<Object>> commit = freeables[i].lab.commit(efsyncOnFlush, false);
                                        LOG.info("freeHeap waiting on commit..." + freeables[i].lab.name());
                                        for (Future<Object> c : commit) {
                                            c.get();
                                        }
                                        stats.gcCommit.increment();
                                    } catch (LABCorruptedException | LABClosedException x) {
                                        LOG.error("Failed to commit.", x);
                                    } catch (Exception x) {
                                        this.committableLabs.compute(freeables[i].lab,
                                            (t, u) -> u == null ? efsyncOnFlush : (boolean) u || efsyncOnFlush);
                                        throw x;
                                    }
                                }
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

    void close(LAB lab) {
        committableLabs.remove(lab);
    }

}
