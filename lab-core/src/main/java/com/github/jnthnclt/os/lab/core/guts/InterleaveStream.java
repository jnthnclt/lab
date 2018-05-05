package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.guts.api.Next;
import com.github.jnthnclt.os.lab.core.guts.api.RawEntryStream;
import com.github.jnthnclt.os.lab.core.guts.api.Scanner;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import java.util.PriorityQueue;

/**
 * @author jonathan.colt
 */
public class InterleaveStream implements Scanner {

    private final Rawhide rawhide;
    private final PriorityQueue<InterleavingStreamFeed> interleavingStreamFeeds;
    private InterleavingStreamFeed active;
    private InterleavingStreamFeed until;


    public InterleaveStream(Rawhide rawhide, PriorityQueue<InterleavingStreamFeed> interleavingStreamFeeds) throws Exception {
        this.rawhide = rawhide;
        this.interleavingStreamFeeds = interleavingStreamFeeds;
    }

    @Override
    public void close() throws Exception {
        if (active != null) {
            active.close();
        }
        for (InterleavingStreamFeed interleavingStreamFeed : interleavingStreamFeeds) {
            interleavingStreamFeed.close();
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
                interleavingStreamFeeds.add(active);
            }

            active = interleavingStreamFeeds.poll();
            if (active == null) {
                return Next.eos;
            }

            while (true) {
                InterleavingStreamFeed first = interleavingStreamFeeds.peek();
                if (first == null
                    || compare(first, active) != 0) {
                    until = first;
                    break;
                }

                interleavingStreamFeeds.poll();
                if (first.feedNext() != null) {
                    interleavingStreamFeeds.add(first);
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

    private int compare(InterleavingStreamFeed left, InterleavingStreamFeed right) throws Exception {
        return rawhide.compareKey(left.nextReadKeyFormatTransformer,
            left.nextReadValueFormatTransformer,
            left.nextRawEntry,
            left.entryKeyBuffer,
            right.nextReadKeyFormatTransformer,
            right.nextReadValueFormatTransformer,
            right.nextRawEntry,
            right.entryKeyBuffer);
    }

}
