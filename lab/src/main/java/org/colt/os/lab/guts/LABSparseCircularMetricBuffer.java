package org.colt.os.lab.guts;

import java.util.Arrays;
import java.util.concurrent.atomic.LongAdder;

/**
 * Supports storing a hits waveform. You can add hit @ time in any order.
 * The Buffer will slide to accommodate hits.
 *
 * NOT thread safe any more, synchronize externally.
 *
 * @author jonathan
 */
public class LABSparseCircularMetricBuffer {

    private long mostRecentTimeStamp = Long.MIN_VALUE;
    private long oldestBucketNumber = Long.MIN_VALUE;
    private long youngestBucketNumber;
    private final long utcOffset; // shifts alignment of buckets as offset from UTC, e.g. aligning with start of work day in PST
    private final long bucketWidthMillis;
    private int cursor; // always points oldest bucket. cursor - 1 is the newestBucket
    private final int numberOfBuckets;
    private volatile double[] metric;
    private long grandTotal = 0;

    public LABSparseCircularMetricBuffer(int numberOfBuckets, long utcOffset, long bucketWidthMillis) {
        this.numberOfBuckets = numberOfBuckets;
        this.utcOffset = utcOffset;
        this.bucketWidthMillis = bucketWidthMillis;
        metric = new double[numberOfBuckets];
        Arrays.fill(metric, Double.NaN);
    }

    public long mostRecentTimestamp() {
        return mostRecentTimeStamp;
    }

    public long duration() {
        return bucketWidthMillis * numberOfBuckets;
    }

    public void set(long time, LongAdder value) {
        if (time > mostRecentTimeStamp) {
            mostRecentTimeStamp = time;
        }
        long absBucketNumber = absBucketNumber(time);
        if (oldestBucketNumber == Long.MIN_VALUE) {
            oldestBucketNumber = absBucketNumber - (numberOfBuckets - 1);
            youngestBucketNumber = absBucketNumber;
        } else {
            if (absBucketNumber < oldestBucketNumber) {
                return;
            }
            if (absBucketNumber > youngestBucketNumber) {
                // we need to slide the buffer to accommodate younger values
                long delta = absBucketNumber - youngestBucketNumber;
                for (int i = 0; i < delta; i++) {
                    metric[cursor] = Double.NaN; // zero out oldest
                    cursor = nextCursor(cursor, 1); // move cursor
                }
                oldestBucketNumber += delta;
                youngestBucketNumber = absBucketNumber;
            }
        }
        int delta = (int) (absBucketNumber - oldestBucketNumber);
        long v = value.longValue();
        grandTotal = v;
        int nextCursor = nextCursor(cursor, delta);
        metric[nextCursor] = v;
    }

    public void add(long time, LongAdder value) {
        if (time > mostRecentTimeStamp) {
            mostRecentTimeStamp = time;
        }
        long absBucketNumber = absBucketNumber(time);
        if (oldestBucketNumber == Long.MIN_VALUE) {
            oldestBucketNumber = absBucketNumber - (numberOfBuckets - 1);
            youngestBucketNumber = absBucketNumber;
        } else {
            if (absBucketNumber < oldestBucketNumber) {
                return;
            }
            if (absBucketNumber > youngestBucketNumber) {
                // we need to slide the buffer to accommodate younger values
                long delta = absBucketNumber - youngestBucketNumber;
                for (int i = 0; i < delta; i++) {
                    metric[cursor] = Double.NaN; // zero out oldest
                    cursor = nextCursor(cursor, 1); // move cursor
                }
                oldestBucketNumber += delta;
                youngestBucketNumber = absBucketNumber;
            }
        }
        int delta = (int) (absBucketNumber - oldestBucketNumber);
        long sumThenReset = value.sumThenReset();
        grandTotal += sumThenReset;
        int nextCursor = nextCursor(cursor, delta);
        if (Double.isNaN(metric[nextCursor])) {
            metric[nextCursor] = sumThenReset;
        } else {
            metric[nextCursor] += sumThenReset;
        }
    }

    public void reset() {
        metric = new double[numberOfBuckets];
        Arrays.fill(metric, Double.NaN);
    }

    public long total() {
        return grandTotal;
    }

    public double last() {
        int c = cursor;
        if (c > 0) {
            c = c - 1;
        } else {
            c = numberOfBuckets - 1;
        }
        double m = metric[c];
        if (Double.isNaN(m)) {
            return 0d;
        }
        return m;
    }

    private long absBucketNumber(long time) {
        long absBucketNumber = time / bucketWidthMillis;
        long absNearestEdge = bucketWidthMillis * absBucketNumber;
        long remainder = time - (absNearestEdge);
        if (remainder < utcOffset) {
            return absBucketNumber - 1;
        } else {
            return absBucketNumber;
        }
    }

    private int nextCursor(int cursor, int move) {
        cursor += move;
        if (cursor >= numberOfBuckets) {
            cursor -= numberOfBuckets;
        }
        return cursor;
    }

    public double[] metric() {
        double[] copy = new double[numberOfBuckets];
        int c = cursor;
        for (int i = 0; i < numberOfBuckets; i++) {
            double m = metric[c];
            copy[i] = Double.isNaN(m) ? 0d : m;
            c = nextCursor(c, 1);
        }
        return copy;
    }

}
