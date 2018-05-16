package com.github.jnthnclt.os.lab.core;

import com.github.jnthnclt.os.lab.core.guts.LABSparseCircularMetricBuffer;
import com.github.jnthnclt.os.lab.core.util.LABLogger;
import com.github.jnthnclt.os.lab.core.util.LABLoggerFactory;
import com.google.common.collect.Maps;
import java.lang.management.ManagementFactory;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

/**
 *
 * @author jonathan.colt
 */
public class LABStats {

    private static final LABLogger LOG = LABLoggerFactory.getLogger();;

    public final LongAdder debt = new LongAdder();
    public final LongAdder open = new LongAdder();
    public final LongAdder closed = new LongAdder();

    public final LongAdder append = new LongAdder();
    public final LongAdder journaledAppend = new LongAdder();

    public final LongAdder gets = new LongAdder();
    public final LongAdder rangeScan = new LongAdder();
    public final LongAdder multiRangeScan = new LongAdder();
    public final LongAdder rowScan = new LongAdder();

    public final LongAdder merging = new LongAdder();
    public final LongAdder merged = new LongAdder();
    public final LongAdder spliting = new LongAdder();
    public final LongAdder splits = new LongAdder();

    public final LongAdder slabbed = new LongAdder();
    public final LongAdder allocationed = new LongAdder();
    public final LongAdder released = new LongAdder();
    public final LongAdder freed = new LongAdder();

    public final LongAdder gc = new LongAdder();
    public final LongAdder gcCommit = new LongAdder();
    public final LongAdder pressureCommit = new LongAdder();
    public final LongAdder commit = new LongAdder();
    public final LongAdder fsyncedCommit = new LongAdder();

    public final LongAdder bytesWrittenToWAL = new LongAdder();
    public final LongAdder bytesWrittenAsIndex = new LongAdder();
    public final LongAdder bytesWrittenAsSplit = new LongAdder();
    public final LongAdder bytesWrittenAsMerge = new LongAdder();

    public final LABSparseCircularMetricBuffer mDebt;
    public final LABSparseCircularMetricBuffer mOpen;
    public final LABSparseCircularMetricBuffer mClosed;

    public final LABSparseCircularMetricBuffer mAppend;
    public final LABSparseCircularMetricBuffer mJournaledAppend;

    public final LABSparseCircularMetricBuffer mGets;
    public final LABSparseCircularMetricBuffer mRangeScan;
    public final LABSparseCircularMetricBuffer mMultiRangeScan;
    public final LABSparseCircularMetricBuffer mRowScan;

    public final LABSparseCircularMetricBuffer mMerging;
    public final LABSparseCircularMetricBuffer mMerged;
    public final LABSparseCircularMetricBuffer mSplitings;
    public final LABSparseCircularMetricBuffer mSplits;

    public final LABSparseCircularMetricBuffer mSlabbed;
    public final LABSparseCircularMetricBuffer mAllocationed;
    public final LABSparseCircularMetricBuffer mReleased;
    public final LABSparseCircularMetricBuffer mFreed;

    public final LABSparseCircularMetricBuffer mGC;
    public final LABSparseCircularMetricBuffer mCommit;
    public final LABSparseCircularMetricBuffer mGCCommit;
    public final LABSparseCircularMetricBuffer mPressureCommit;
    public final LABSparseCircularMetricBuffer mFsyncedCommit;

    public final LABSparseCircularMetricBuffer mBytesWrittenToWAL;
    public final LABSparseCircularMetricBuffer mBytesWrittenAsIndex;
    public final LABSparseCircularMetricBuffer mBytesWrittenAsSplit;
    public final LABSparseCircularMetricBuffer mBytesWrittenAsMerge;

    public final ConcurrentMap<String, Written> writtenBrokenDownByName = Maps.newConcurrentMap();

    private final int numberOfBuckets;
    private final long utcOffset;
    private final long bucketWidthMillis;

    public LABStats() {
        this(180, 0, 10_000);
    }

    public LABStats(int numberOfBuckets, long utcOffset, long bucketWidthMillis) {

        this.numberOfBuckets = numberOfBuckets;
        this.utcOffset = utcOffset;
        this.bucketWidthMillis = bucketWidthMillis;

        this.mDebt = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);
        this.mOpen = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);
        this.mClosed = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);

        this.mAppend = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);
        this.mJournaledAppend = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);

        this.mGets = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);
        this.mRangeScan = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);
        this.mMultiRangeScan = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);
        this.mRowScan = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);

        this.mMerging = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);
        this.mMerged = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);
        this.mSplitings = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);
        this.mSplits = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);

        this.mSlabbed = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);
        this.mAllocationed = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);
        this.mReleased = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);
        this.mFreed = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);

        this.mGC = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);
        this.mCommit = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);
        this.mGCCommit = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);
        this.mPressureCommit = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);
        this.mFsyncedCommit = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);

        this.mBytesWrittenToWAL = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);
        this.mBytesWrittenAsIndex = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);
        this.mBytesWrittenAsSplit = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);
        this.mBytesWrittenAsMerge = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);


        register("files>debt", new LongAdderCounter(debt));
        register("files>open", new LongAdderCounter(open));
        register("files>closed", new LongAdderCounter(closed));

        register("append>append", new LongAdderCounter(append));
        register("append>journaledAppend", new LongAdderCounter(journaledAppend));

        register("read>gets", new LongAdderCounter(gets));
        register("read>rangeScan", new LongAdderCounter(rangeScan));
        register("read>multiRangeScan", new LongAdderCounter(multiRangeScan));
        register("read>rowScan", new LongAdderCounter(rowScan));

        register("lsm>merging", new LongAdderCounter(merging));
        register("lsm>merged", new LongAdderCounter(merged));
        register("lsm>spliting", new LongAdderCounter(spliting));
        register("lsm>splits", new LongAdderCounter(splits));

        register("memory.slabbed", new LongAdderCounter(slabbed));
        register("memory.allocationed", new LongAdderCounter(allocationed));
        register("memory.released", new LongAdderCounter(released));
        register("memory.freed", new LongAdderCounter(freed));


        register("commits>gc", new LongAdderCounter(freed));
        register("commits>gcCommit", new LongAdderCounter(freed));
        register("commits>pressureCommit", new LongAdderCounter(freed));
        register("commits>commit", new LongAdderCounter(freed));
        register("commits>fsyncedCommit", new LongAdderCounter(freed));

        register("bytes>writtenToWAL", new LongAdderCounter(freed));
        register("bytes>writtenAsIndex", new LongAdderCounter(freed));
        register("bytes>writtenAsSplit", new LongAdderCounter(freed));
        register("bytes>writtenAsMerge", new LongAdderCounter(freed));

    }

    public void refresh() {
        long timestamp = System.currentTimeMillis();
        mDebt.add(timestamp, debt);
        mOpen.add(timestamp, open);
        mClosed.add(timestamp, closed);

        mAppend.add(timestamp, append);
        mJournaledAppend.add(timestamp, journaledAppend);

        mGets.add(timestamp, gets);
        mRangeScan.add(timestamp, rangeScan);
        mMultiRangeScan.add(timestamp, multiRangeScan);
        mRowScan.add(timestamp, rowScan);

        mMerging.add(timestamp, merging);
        mMerged.add(timestamp, merged);
        mSplitings.add(timestamp, spliting);
        mSplits.add(timestamp, splits);

        mSlabbed.add(timestamp, slabbed);
        mAllocationed.add(timestamp, allocationed);
        mReleased.add(timestamp, released);
        mFreed.add(timestamp, freed);

        mPressureCommit.add(timestamp, pressureCommit);
        mCommit.add(timestamp, commit);
        mFsyncedCommit.add(timestamp, fsyncedCommit);
        mGC.add(timestamp, gc);
        mGCCommit.add(timestamp, gcCommit);

        mBytesWrittenToWAL.add(timestamp, bytesWrittenToWAL);
        mBytesWrittenAsIndex.add(timestamp, bytesWrittenAsIndex);
        mBytesWrittenAsSplit.add(timestamp, bytesWrittenAsSplit);
        mBytesWrittenAsMerge.add(timestamp, bytesWrittenAsMerge);

        for (Written value : writtenBrokenDownByName.values()) {
            value.refresh(timestamp);
        }
    }

    public void written(String key, int power) {
        Written written = writtenBrokenDownByName.computeIfAbsent(key, (t) -> new Written(numberOfBuckets, utcOffset, bucketWidthMillis));
        written.entriesWritten.increment();
        written.entriesWrittenBatchPower[power].increment();
    }

    public static class Written {

        public final LongAdder entriesWritten = new LongAdder();
        public final LABSparseCircularMetricBuffer mEntriesWritten;
        public final LongAdder[] entriesWrittenBatchPower = new LongAdder[32];
        public final LABSparseCircularMetricBuffer[] mEntriesWrittenBatchPower = new LABSparseCircularMetricBuffer[32];

        public Written(int numberOfBuckets, long utcOffset, long bucketWidthMillis) {
            for (int i = 0; i < 32; i++) {
                entriesWrittenBatchPower[i] = new LongAdder();
                mEntriesWrittenBatchPower[i] = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);
            }
            this.mEntriesWritten = new LABSparseCircularMetricBuffer(numberOfBuckets, utcOffset, bucketWidthMillis);
        }

        public void refresh(long timestamp) {
            mEntriesWritten.add(timestamp, entriesWritten);
            for (int i = 0; i < 32; i++) {
                mEntriesWrittenBatchPower[i].add(timestamp, entriesWrittenBatchPower[i]);
            }
        }

    }

    public class LongAdderCounter implements LABCounterMXBean {
        private final LongAdder longAdder;

        public LongAdderCounter(LongAdder longAdder) {
            this.longAdder = longAdder;
        }

        @Override
        public long getValue() {
            return longAdder.longValue();
        }

        @Override
        public String getType() {
            return "count";
        }
    }

    public interface LABCounterMXBean {

        long getValue();

        String getType();

    }


    private static void register(String name, Object mbean) {
        name = name.replace(':', '_');

        String[] parts = name.split(">");

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append("leaf");
            sb.append(i);
            sb.append("=");
            sb.append(parts[i]);
        }

        Class clazz = mbean.getClass();
        String objectName = "lab.metrics:type=" + clazz.getSimpleName() + "," + sb.toString();

        LOG.debug("registering bean: " + objectName);

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        try {
            ObjectName mbeanName = new ObjectName(objectName);

            // note: unregister any previous, as this may be a replacement
            if (mbs.isRegistered(mbeanName)) {
                mbs.unregisterMBean(mbeanName);
            }

            mbs.registerMBean(mbean, mbeanName);

            LOG.debug("registered bean: " + objectName);
        } catch (MalformedObjectNameException | NotCompliantMBeanException |
            InstanceAlreadyExistsException | InstanceNotFoundException | MBeanRegistrationException e) {
            LOG.warn("unable to register bean: " + objectName + "cause: " + e.getMessage(), e);
        }
    }

}
