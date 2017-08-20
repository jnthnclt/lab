package org.colt.os.lab.guts;

/**
 *
 * @author jonathan.colt
 */
public class MergeRange {

    final long generation;
    final int offset;
    final int length;

    public MergeRange(long generation, int startOfSmallestMerge, int length) {
        this.generation = generation;
        this.offset = startOfSmallestMerge;
        this.length = length;
    }

    @Override
    public String toString() {
        return "MergeRange{"
            + "generation=" + generation
            + ", offset=" + offset
            + ", length=" + length
            + '}';
    }
}
