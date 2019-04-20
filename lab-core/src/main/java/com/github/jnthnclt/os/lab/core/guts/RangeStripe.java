package com.github.jnthnclt.os.lab.core.guts;

class RangeStripe {

    final KeyRange keyRange;
    final CompactableIndexes mergeableIndexes;

    public RangeStripe(KeyRange keyRange, CompactableIndexes mergeableIndexes) {
        this.keyRange = keyRange;
        this.mergeableIndexes = mergeableIndexes;
    }

}
