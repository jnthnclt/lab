package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.core.api.FormatTransformer;
import com.github.jnthnclt.os.lab.core.guts.api.Next;
import java.util.PriorityQueue;
import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.guts.api.RawEntryStream;
import com.github.jnthnclt.os.lab.core.guts.api.ReadIndex;
import com.github.jnthnclt.os.lab.core.guts.api.Scanner;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;

/**
 * @author jonathan.colt
 */
public class InterleaveStream implements Scanner {

    private final Rawhide rawhide;
    private final PriorityQueue<Feed> feeds = new PriorityQueue<>();
    private Feed active;
    private Feed until;


    public InterleaveStream(ReadIndex[] indexs, byte[] from, byte[] to, Rawhide rawhide) throws Exception {
        this.rawhide = rawhide;
        boolean rowScan = from == null && to == null;
        for (int i = 0; i < indexs.length; i++) {
            Scanner scanner = null;
            try {
                if (rowScan) {
                    scanner = indexs[i].rowScan(new ActiveScanRow(), new BolBuffer(), new BolBuffer());
                } else {
                    scanner = indexs[i].rangeScan(new ActiveScanRange(false), from, to, new BolBuffer(), new BolBuffer());
                }
                if (scanner != null) {
                    Feed feed = new Feed(i, scanner, rawhide);
                    feed.feedNext();
                    feeds.add(feed);
                }
            } catch (Throwable t) {
                if (scanner != null) {
                    scanner.close();
                }
                throw t;
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (active != null) {
            active.close();
        }
        for (Feed feed : feeds) {
            feed.close();
        }
    }

    public boolean stream(RawEntryStream stream) throws Exception {

        Next more = Next.more;
        while (more == Next.more) {
            more = next(stream, null);
        }
        return more != Next.stopped;
    }

    @Override
    public Next next(RawEntryStream stream, BolBuffer nextHint) throws Exception {

        // 0.     3, 5, 7, 9
        // 1.     3, 4, 7, 10
        // 2.     3, 6, 8, 11
        if (active == null || until != null && compare(active, until) >= 0) {

            if (active != null) {
                feeds.add(active);
            }

            active = feeds.poll();
            if (active == null) {
                return Next.eos;
            }

            while (true) {
                Feed first = feeds.peek();
                if (first == null
                    || compare(first, active) != 0) {
                    until = first;
                    break;
                }

                feeds.poll();
                if (first.feedNext() != null) {
                    feeds.add(first);
                } else {
                    first.close();
                }
            }
        }

        if (active != null) {
            if (active.nextRawEntry != null) {
                if (!stream.stream(active.nextReadKeyFormatTransformer,
                    active.nextReadValueFormatTransformer,
                    active.nextRawEntry)) {
                    return Next.stopped;
                }
            }
            if (active.feedNext() == null) {
                active.close();
                active = null;
                until = null;
            }
            return Next.more;
        } else {
            return Next.eos;
        }
    }

    private int compare(Feed left, Feed right) throws Exception {
        return rawhide.compareKey(left.nextReadKeyFormatTransformer,
            left.nextReadValueFormatTransformer,
            left.nextRawEntry,
            left.entryKeyBuffer,
            right.nextReadKeyFormatTransformer,
            right.nextReadValueFormatTransformer,
            right.nextRawEntry,
            right.entryKeyBuffer);
    }

    private static class Feed implements Comparable<Feed>, RawEntryStream {

        private final int index;
        private final Scanner scanner;
        private final Rawhide rawhide;
        private final BolBuffer entryKeyBuffer = new BolBuffer();

        private FormatTransformer nextReadKeyFormatTransformer;
        private FormatTransformer nextReadValueFormatTransformer;
        private BolBuffer nextRawEntry;

        public Feed(int index, Scanner scanner, Rawhide rawhide) {
            this.index = index;
            this.scanner = scanner;
            this.rawhide = rawhide;
        }

        private BolBuffer feedNext() throws Exception {
            Next hadNext = scanner.next(this, null);
            if (hadNext != Next.more) {
                nextRawEntry = null;
            }
            return nextRawEntry;
        }

        @Override
        public boolean stream(FormatTransformer readKeyFormatTransformer, FormatTransformer readValueFormatTransformer, BolBuffer rawEntry) throws Exception {
            nextRawEntry = rawEntry;
            nextReadKeyFormatTransformer = readKeyFormatTransformer;
            nextReadValueFormatTransformer = readValueFormatTransformer;
            return true;
        }

        @Override
        public int compareTo(Feed o) {
            try {
                int c = rawhide.mergeCompare(nextReadKeyFormatTransformer,
                    nextReadValueFormatTransformer,
                    nextRawEntry,
                    entryKeyBuffer,
                    o.nextReadKeyFormatTransformer,
                    o.nextReadValueFormatTransformer,
                    o.nextRawEntry,
                    o.entryKeyBuffer);

                if (c != 0) {
                    return c;
                }
                c = Integer.compare(index, o.index);
                return c;
            } catch (Exception x) {
                throw new RuntimeException(x);
            }
        }

        private void close() throws Exception {
            scanner.close();
        }
    }

}
