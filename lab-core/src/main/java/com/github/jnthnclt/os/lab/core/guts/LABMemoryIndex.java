package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.base.BolBuffer;
import com.github.jnthnclt.os.lab.core.LABHeapPressure;
import com.github.jnthnclt.os.lab.core.LABStats;
import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.guts.LABMap.Compute;
import com.github.jnthnclt.os.lab.core.guts.allocators.LABCostChangeInBytes;
import com.github.jnthnclt.os.lab.core.guts.api.AppendEntries;
import com.github.jnthnclt.os.lab.core.guts.api.AppendEntryStream;
import com.github.jnthnclt.os.lab.core.guts.api.RawAppendableIndex;
import com.github.jnthnclt.os.lab.core.guts.api.ReadIndex;
import com.github.jnthnclt.os.lab.core.guts.api.Scanner;
import com.github.jnthnclt.os.lab.log.LABLogger;
import com.github.jnthnclt.os.lab.log.LABLoggerFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class LABMemoryIndex implements RawAppendableIndex {

    private static final LABLogger LOG = LABLoggerFactory.getLogger();

    private final LABMap<BolBuffer, BolBuffer> index;
    private final AtomicLong approximateCount = new AtomicLong();
    private final AtomicBoolean disposed = new AtomicBoolean(false);

    private final ExecutorService destroy;
    private final LABHeapPressure heapPressure;
    private final AtomicLong costInBytes = new AtomicLong();

    private final Rawhide rawhide;

    private final int numBones = Short.MAX_VALUE;
    private final Semaphore hideABone = new Semaphore(numBones, true);
    private final ReadIndex reader;

    private final Object timestampLock = new Object();
    private volatile long maxTimestamp = -1;
    private volatile long maxVersion = -1;
    private final LABCostChangeInBytes costChangeInBytes;
    private final Compute<BolBuffer, BolBuffer> compute;

    public LABMemoryIndex(ExecutorService destroy,
        LABHeapPressure heapPressure,
        LABStats stats,
        Rawhide rawhide,
        LABMap<BolBuffer, BolBuffer> index) {

        this.costChangeInBytes = (allocated, resued) -> {
            costInBytes.addAndGet(allocated);
            heapPressure.change(allocated);
            stats.released.add(-resued);
        };

        this.compute = (rawEntryBuffer, value) -> {

            BolBuffer merged;
            if (value == null) {
                approximateCount.incrementAndGet();
                merged = rawEntryBuffer;
            } else {
                merged = rawhide.merge(value,
                    rawEntryBuffer);

            }
            long timestamp = rawhide.timestamp(merged);
            long version = rawhide.version( merged);

            synchronized (timestampLock) {
                if (rawhide.isNewerThan(timestamp, version, maxTimestamp, maxVersion)) {
                    maxTimestamp = timestamp;
                    maxVersion = version;
                }
            }

            return merged;
        };

        this.destroy = destroy;
        this.heapPressure = heapPressure;
        this.rawhide = rawhide;
        this.index = index;
        this.reader = new ReadIndex() {

            @Override
            public BolBuffer pointScan(boolean hashIndexEnabled, byte[] key) throws Exception {
                BolBuffer got = index.get(new BolBuffer(key), new BolBuffer());
                if (got == null) {
                    return null;
                }
                return got;
            }

            @Override
            public Scanner rangeScan(boolean hashIndexEnabled, boolean pointFrom, byte[] from, byte[] to, BolBuffer entryBuffer, BolBuffer entryKeyBuffer) throws Exception {
                return index.scanner(pointFrom, from, to, entryBuffer, entryKeyBuffer);
            }

            @Override
            public Scanner rowScan(BolBuffer entryBuffer, BolBuffer entryKeyBuffer) throws Exception {
                return index.scanner(false,null, null, entryBuffer, entryKeyBuffer);
            }

            @Override
            public long count() {
                throw new UnsupportedOperationException("NOPE");
            }

            @Override
            public void release() {
                hideABone.release();
            }
        };
    }


    public int poweredUpTo() {
        return index.poweredUpTo();
    }

    public boolean containsKeyInRange(byte[] from, byte[] to) throws Exception {
        return index.contains(from, to);
    }

    @Override
    public boolean append(AppendEntries entries, BolBuffer keyBuffer) throws Exception {
        BolBuffer valueBuffer = new BolBuffer(); // Grrrr

        AppendEntryStream appendEntryStream = (rawEntryBuffer) -> {
            BolBuffer key = rawhide.key(rawEntryBuffer, keyBuffer);
            index.compute(rawEntryBuffer, key, valueBuffer, compute, costChangeInBytes);
            return true;
        };
        return entries.consume(appendEntryStream);
    }

    // you must call release on this reader! Try to only use it as long as you have to!
    public ReadIndex acquireReader() throws Exception {
        hideABone.acquire();
        if (disposed.get()) {
            hideABone.release();
            return null;
        }
        return reader;
    }

    public void destroy() {
        destroy.submit(() -> {
            hideABone.acquire(numBones);
            try {
                disposed.set(true);
                index.clear();
                long cost = costInBytes.get();
                heapPressure.change(-cost);
                costInBytes.addAndGet(-cost);
            } catch (Throwable t) {
                LOG.error("Destroy failed horribly!", t);
                throw t;
            } finally {
                hideABone.release(numBones);
            }
            return null;
        });
    }

    public void closeReadable() {
    }

    @Override
    public void closeAppendable(boolean fsync) throws Exception {
    }

    public boolean isEmpty() throws Exception {
        return index.isEmpty();
    }

    public long count() {
        return approximateCount.get();
    }

    public long sizeInBytes() {
        return costInBytes.get();
    }

    public boolean mightContain(long newerThanTimestamp, long newerThanVersion) {

        synchronized (timestampLock) {
            return rawhide.mightContain(maxTimestamp,
                maxVersion,
                newerThanTimestamp,
                newerThanVersion);
        }

    }

    public byte[] minKey() throws Exception {
        return index.firstKey();
    }

    public byte[] maxKey() throws Exception {
        return index.lastKey();
    }

}
