package com.github.jnthnclt.os.lab.core.guts;

import java.io.File;

/**
 * @author jonathan.colt
 */
public class IndexRangeId implements Comparable<IndexRangeId> {

    final long start;
    final long end;
    final long generation;

    public IndexRangeId(long start, long end, long generation) {
        this.start = start;
        this.end = end;
        this.generation = generation;
    }

    public boolean intersects(IndexRangeId range) {
        return start <= range.end && range.start <= end;
    }

    @Override
    public int compareTo(IndexRangeId o) {
        int c = Long.compare(start, o.start);
        if (c == 0) {
            c = Long.compare(o.end, end); // reversed
        }
        return c;
    }

    @Override
    public String toString() {
        return "(" + start + " - " + end + " - " + generation + ')';
    }

    public File toFile(File parent) {
        return new File(parent, start + "-" + end + "-" + generation);
    }

    public IndexRangeId join(IndexRangeId id, long generation) {
        return new IndexRangeId(Math.min(start, id.start), Math.max(end, id.end), generation);
    }

}
