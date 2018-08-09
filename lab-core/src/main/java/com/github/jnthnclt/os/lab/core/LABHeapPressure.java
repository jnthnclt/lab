package com.github.jnthnclt.os.lab.core;

import com.github.jnthnclt.os.lab.core.api.exceptions.LABClosedException;
import com.github.jnthnclt.os.lab.core.api.exceptions.LABCorruptedException;
import com.github.jnthnclt.os.lab.log.LABLogger;
import com.github.jnthnclt.os.lab.log.LABLoggerFactory;
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
    private final AtomicLong changed = new AtomicLong();
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

        Preconditions.checkArgument(maxHeapPressureInBytes < blockOnHeapPressureInBytes,
            "maxHeapPressureInBytes must be less than blockOnHeapPressureInBytes");

        this.schedule.submit(() -> {
            while (true) {
                try {
                    freeHeap();
                    Thread.sleep(1000); // hmm
                } catch (Exception x) {
                    LOG.error("Failure monitoring lab heap", x);
                }
            }
        });
    }


    private void freeHeap() {

        try {
            stats.commitable.set(committableLabs.size());
            long globalHeap = globalHeapCostInBytes.get();
            if (globalHeap < maxHeapPressureInBytes) {
                return;
            }

            long greed = (long) (maxHeapPressureInBytes * 0.50); // TODO config?
            AtomicLong debtInBytes = new AtomicLong(globalHeap - greed);
            stats.gc.increment();

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

            synchronized (globalHeapCostInBytes) {
                globalHeapCostInBytes.notifyAll();
            }

        } catch (Exception x) {
            LOG.warn("Free heap encountered an error.", x);
        }
    }

    public void change(long delta) {
        changed.incrementAndGet();
        globalHeapCostInBytes.addAndGet(delta);
        if (delta < 0) {
            stats.freed.add(-delta);
        } else {
            stats.slabbed.add(delta);
        }
    }

    private volatile long slowing = -1;

    void commitIfNecessary(LAB lab, long labMaxHeapPressureInBytes, boolean fsyncOnFlush) throws Exception {
        committableLabs.compute(lab, (t, u) -> u == null ? fsyncOnFlush : u || fsyncOnFlush);
        //System.out.println((maxHeapPressureInBytes-globalHeapCostInBytes.get())+" maxed out "+(globalHeapCostInBytes.get() > maxHeapPressureInBytes));
        long heap = globalHeapCostInBytes.get();
        if (heap >= blockOnHeapPressureInBytes) {
            synchronized (globalHeapCostInBytes) {
                LOG.warn("Blocking writes '{}' because heap pressure is maxed out!", lab.name());
                globalHeapCostInBytes.wait();
                LOG.info("Resuming writes '{}'", lab.name());
            }
        } else if (heap >= maxHeapPressureInBytes) {
            double percent = (heap - maxHeapPressureInBytes) / (double) (blockOnHeapPressureInBytes - maxHeapPressureInBytes);
            percent = Math.min(percent, 1.0);
            long slow = (int) (percent * 1000); // TODO config
            if (slowing != slow) {
                slowing = slow;
                stats.appendSlowed.increment();
                LOG.debug("Slowing writes '{}' by {} millis because of heap pressure", lab.name(), slow);
            }
            Thread.sleep(slow);
        } else {
            slowing = -1;
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

    void close(LAB lab) {
        committableLabs.remove(lab);
    }

}
