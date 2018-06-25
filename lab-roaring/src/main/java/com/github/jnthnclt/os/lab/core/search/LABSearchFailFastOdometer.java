package com.github.jnthnclt.os.lab.core.search;

import com.github.jnthnclt.os.lab.core.search.LABSearch.CachedFieldValue;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.roaringbitmap.RoaringBitmap;

public class LABSearchFailFastOdometer {

    public static <E> FailFastdometer<CachedFieldValue, E> buildOdometer(LABSearchIndex index,
        List<String> fieldNames,
        List<List<String>> fieldValues,
        boolean allowWildcards) throws Exception {

        if (fieldNames.isEmpty() || index == null) {
            return null;
        }

        List<Expansion> expansions = Lists.newArrayList();
        for (int i = fieldNames.size() - 1; i > -1; i--) {
            String fieldName = fieldNames.get(i);
            Set<String> set = fieldSet(index, allowWildcards, fieldName, fieldValues.get(i));
            expansions.add(new Expansion(fieldName, set));
        }
        Collections.sort(expansions);

        FailFastdometer<CachedFieldValue, E> failFastdometer = null;
        for (Expansion expansion : expansions) {
            List<CachedFieldValue> cachedFieldValues = expansion.values.stream()
                .map(s -> new CachedFieldValue(expansion.name, s))
                .collect(Collectors.toList());

            List<CachedFieldValue> values = Lists.newArrayList(cachedFieldValues);
            for (int i = 0; i < values.size(); i++) {
                if (values.get(i).value == null) {
                    values.set(i, null);
                }
            }
            failFastdometer = new FailFastdometer<>(values, failFastdometer);
        }

        return failFastdometer;
    }

    private static Set<String> fieldSet(LABSearchIndex index, boolean allowWildcards, String expansion, List<String> expansionValue) throws Exception {
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

    public static class FailFastOdometerResult<V, E> {
        public final List<V> pattern;
        public final E edge;

        public FailFastOdometerResult(List<V> pattern, E edge) {
            this.pattern = pattern;
            this.edge = edge;
        }
    }

    public interface FailFastOdometerEdge<V, E> {
        E compute(V prior, E priorResult, V current);
    }

    public static class FailFastdometer<V, E> {

        public final List<V> values;
        public final FailFastdometer<V, E> next;
        private int i = 0;
        private E edge;
        private V edgeValue;

        public FailFastdometer(List<V> values, FailFastdometer<V, E> next) {
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
            return i < values.size() && (next != null ? next.hasNext() : true);
        }

        public FailFastOdometerResult<V, E> next(List<V> parts, V priorValue, E priorResult, FailFastOdometerEdge<V, E> failFastOdometerEdge) {
            E e = internalNext(parts, priorValue, priorResult, failFastOdometerEdge);
            advance();
            if (e == null) {
                return null;
            }
            return new FailFastOdometerResult<>(parts, e);
        }

        private E internalNext(List<V> parts, V priorValue, E priorResult, FailFastOdometerEdge<V, E> failFastOdometerEdge) {
            V currentV = values.get(i);
            parts.add(currentV);
            if (next == null) {
                if (currentV == null) {
                    return priorResult;
                } else {
                    return failFastOdometerEdge.compute(priorValue, priorResult, currentV);
                }
            } else {
                if (this.edge == null) {
                    if (currentV == null) {
                        this.edge = priorResult;
                        this.edgeValue = priorValue;
                    } else {
                        E compute = failFastOdometerEdge.compute(priorValue, priorResult, currentV);
                        if (compute == null) {
                            next.reset();
                            i++;
                            return null;
                        }
                        this.edge = compute;
                        this.edgeValue = currentV;
                    }
                }
                return next.internalNext(parts, this.edgeValue, this.edge, failFastOdometerEdge);
            }
        }

        private void reset() {
            i = 0;
            if (next != null) {
                next.reset();
            }
        }

        private boolean advance() {
            if (next == null) {
                edge = null;
                i++;
                return i < values.size();
            } else if (next.advance()) {
                return true;
            } else {
                edge = null;
                i++;

                boolean hasMore = i < values.size();
                if (hasMore) {
                    next.reset();
                }
                return hasMore;
            }
        }

    }

    private static class Expansion implements Comparable<Expansion> {
        private final String name;
        private final Set<String> values;

        private Expansion(String name, Set<String> values) {
            this.name = name;
            this.values = values;
        }

        @Override
        public int compareTo(Expansion o) {
            int c = -Integer.compare(values.size(), o.values.size());
            if (c != 0) {
                return c;
            }
            return name.compareTo(o.name);
        }
    }


    public static void main(String[] args) {

        Map<String, RoaringBitmap> index = Maps.newHashMap();
        index.put("x", create(1, 2, 3, 4));
        index.put("y", create(1, 2, 3, 4));
        index.put("1", create(1, 2, 3));
        index.put("3", create(5, 6, 7));
        index.put("4", create(1, 2, 3));
        index.put("a", create(1, 2));
        index.put("b", create(1, 2));
        index.put("c", create(1, 2));

        FailFastdometer failFastdometer3 = new FailFastdometer(Arrays.asList("a", "b", "c", null), null);
        FailFastdometer failFastdometer2 = new FailFastdometer(Arrays.asList("1", null, "3", "4"), failFastdometer3);
        FailFastdometer<String, RoaringBitmap> failFastdometer1 = new FailFastdometer<>(Arrays.asList("x", "y"), failFastdometer2);

        List<String> parts = Lists.newArrayList();

        // [a,b,c], []

        RoaringBitmap all = create(1, 2, 3, 4, 5);

        while (failFastdometer1.hasNext()) {
            parts.clear();
            FailFastOdometerResult<String, RoaringBitmap> r = failFastdometer1.next(parts, "all", all,
                (priorValue, priorResult, current) -> {
                    RoaringBitmap got = index.get(current);
                    RoaringBitmap and = RoaringBitmap.and(priorResult, got);
                    System.out.println(priorValue+"->"+current + " " + and);
                    if (and.getCardinality() == 0) {
                        return null;
                    }
                    return and;
                });
            if (r != null) {
                System.out.println("Result:" + r.pattern + " " + r.edge);
            }
        }
    }

    private static final RoaringBitmap create(int... value) {
        RoaringBitmap bitmap = new RoaringBitmap();
        bitmap.add(value);
        return bitmap;
    }
}
