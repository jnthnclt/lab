package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.base.BolBuffer;
import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.guts.api.Scanner;
import java.util.PriorityQueue;

/**
 * @author jonathan.colt
 */
public class InterleaveStream implements Scanner {

    private final Rawhide rawhide;
    private final PriorityQueue<InterleavingStreamFeed> interleavingStreamFeeds;
    private InterleavingStreamFeed active;
    private InterleavingStreamFeed until;


    public InterleaveStream(Rawhide rawhide, PriorityQueue<InterleavingStreamFeed> interleavingStreamFeeds) {
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

    @Override
    public BolBuffer next(BolBuffer rawEntry, BolBuffer nextKeyHint) throws Exception {
        while(true) {
            if (active == null || until != null && compare(active, until) >= 0) {

                if (active == null) {
                    active = interleavingStreamFeeds.poll();
                } else {
                    interleavingStreamFeeds.add(active);
                    active = interleavingStreamFeeds.poll();
                }

                while (true) {
                    InterleavingStreamFeed first = interleavingStreamFeeds.peek();
                    if (first == null || compare(first, active) != 0) {
                        until = first;
                        break;
                    }

                    interleavingStreamFeeds.poll();
                    if (first.feedNext(nextKeyHint) != null) {
                        interleavingStreamFeeds.add(first);
                    } else {
                        first.close();
                    }
                }
            }

            if (active != null) {
                BolBuffer next = active.nextRawEntry;
                if (nextKeyHint != null && compare(active, nextKeyHint) < 0) {
                    if (active.feedNext(nextKeyHint) == null) {
                        active.close();
                        active = null;
                        until = null;
                    }
                    continue;
                } else {
                    if (active.feedNext(nextKeyHint) == null) {
                        active.close();
                        active = null;
                        until = null;
                    }
                }
                return next;
            } else {
                close();
                return null;
            }
        }
    }

    private int compare(InterleavingStreamFeed left, InterleavingStreamFeed right) throws Exception {
        return rawhide.compareKey(
            left.nextRawEntry,
            left.entryKeyBuffer,
            right.nextRawEntry,
            right.entryKeyBuffer);
    }

    private int compare(InterleavingStreamFeed left, BolBuffer right) throws Exception {
        return rawhide.compareKey(
            left.nextRawEntry,
            left.entryKeyBuffer,
            right);
    }

}
