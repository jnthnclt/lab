package com.github.jnthnclt.os.lab.core.search;

import com.github.jnthnclt.os.lab.core.search.LABSearch.CachedFieldValue;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class LABSearchOdometer {

    public static LABSearchCombinator<CachedFieldValue> buildOdometer(LABSearchIndex index,
        List<String> expansions,
        List<List<String>> expansionValues,
        boolean allowWildcards) throws Exception {

        if (expansions.isEmpty() || index == null) {
            return null;
        }
        Odometer<CachedFieldValue> odometer = null;
        for (int i = expansions.size() - 1; i > -1; i--) {
            String expansion = expansions.get(i);
            Set<String> set = fieldSet(index, allowWildcards, expansion, expansionValues.get(i));

            List<CachedFieldValue> fieldValues = set.stream().map(s -> new CachedFieldValue(expansion, s)).collect(Collectors.toList());
            odometer = new Odometer<>(false, fieldValues, odometer);
        }
        // mark last odometer as first
        Odometer<CachedFieldValue> ef_odometer = new Odometer<>(true, odometer.values, odometer.next);
        return new LABSearchCombinator<CachedFieldValue>() {
            @Override
            public boolean hasNext() {
                return ef_odometer.hasNext();
            }

            @Override
            public List<CachedFieldValue> next() {
                return ef_odometer.next();
            }

            @Override
            public long combinations() {
                return ef_odometer.combinations();
            }
        };
    }

    public static Set<String> fieldSet(LABSearchIndex index, boolean allowWildcards, String expansion, List<String> expansionValue) throws Exception {
        Set<String> aFieldValues;
        if (expansionValue != null) {
            if (expansionValue.size() == 1 && "*".equals(expansionValue.get(0))) {
                aFieldValues = Sets.newHashSet();
                if (allowWildcards) {
                    index.fieldStringValues(index.fieldOrdinal(expansion), null, s -> {
                        aFieldValues.add(s);
                        return true;
                    });
                } else {
                    throw new IllegalStateException("Wild cards are not supported");
                }
            } else {
                aFieldValues = Sets.newHashSet(expansionValue);
            }
        } else {
            aFieldValues = Sets.newHashSet();
            index.fieldStringValues(index.fieldOrdinal(expansion), null, s -> {
                aFieldValues.add(s);
                return true;
            });
        }
        return aFieldValues;
    }

    public static class Odometer<V> implements Iterator<List<V>>, Iterable<List<V>> {



        private final boolean first;
        public final List<V> values;
        public final Odometer next;
        private int i = 0;

        public Odometer(boolean first, List<V> values, Odometer next) {
            this.first = first;
            this.values = values;
            this.next = next;
        }

        public long combinations() {
            long c = values.size();
            if (next != null) {
                c *= next.combinations();
            }
            return c;
        }

        public boolean hasNext() {
            if (first && i >= values.size()) {
                return false;
            }
            if (next == null) {
                return i < values.size();
            }
            return next.hasNext();

        }

        public List<V> next() {
            List<V> parts = new ArrayList<>();
            next(parts);
            advance();
            return parts;
        }

        /**
         * @return true if rollover
         */
        private boolean advance() {
            if (next == null) {
                i++;
                if (i >= values.size()) {
                    if (!first) {
                        i = 0;
                    }
                    return true;
                }
                return false;
            } else if (next.advance()) {
                i++;
                if (i >= values.size()) {
                    if (first) {
                        return false;
                    }
                    i = 0;
                    return true;
                }
                return false;
            } else {
                return false;
            }
        }

        private void next(List<V> parts) {
            parts.add(values.get(i));
            if (next == null) {
                return;
            }
            next.next(parts);
        }

        @Override
        public Iterator<List<V>> iterator() {
            return this;
        }
    }
}
