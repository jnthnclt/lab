package com.github.jnthnclt.os.lab.core.search;

import java.util.Iterator;
import java.util.List;

public interface LABSearchCombinator<V> extends Iterator<List<V>> {
    long combinations();
}
