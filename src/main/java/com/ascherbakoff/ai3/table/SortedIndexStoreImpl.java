package com.ascherbakoff.ai3.table;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BiFunction;
import org.jetbrains.annotations.Nullable;

public class SortedIndexStoreImpl<T> implements SortedIndexStore<T> {
    private NavigableMap<Tuple, Set<T>> store = new ConcurrentSkipListMap<>();

    @Override
    public Cursor<Map<Tuple, Cursor<T>>> scan(@Nullable Tuple lower, boolean lowerInclusive, @Nullable Tuple upper, boolean upperInclusve) {
        NavigableMap<Tuple, Set<T>> subMap = store.subMap(lower, lowerInclusive, upper, upperInclusve);

        Set<Entry<Tuple, Set<T>>> entries = subMap.entrySet();

        Iterator<Entry<Tuple, Set<T>>> iter = entries.iterator();

        return new Cursor<Map<Tuple, Cursor<T>>>() {
            @Nullable
            @Override
            public Map<Tuple, Cursor<T>> next() {
                if (!iter.hasNext())
                    return null;

                Entry<Tuple, Set<T>> next = iter.next();
                return Map.of(next.getKey(), Cursor.<T>wrap(next.getValue().iterator()));
            }
        };
    }

    @Override
    public boolean insert(Tuple key, T rowId) {
        final boolean[] inserted = new boolean[1];

        store.compute(key, new BiFunction<Tuple, Set<T>, Set<T>>() {
            @Override
            public Set<T> apply(Tuple tuple, Set<T> ts) {
                if (ts == null) {
                    ts = Collections.newSetFromMap(new ConcurrentHashMap<>());
                }

                inserted[0] = ts.add(rowId);

                return ts;
            }
        });

        return inserted[0];
    }

    @Override
    public boolean remove(Tuple key, T rowId) {
        final boolean[] removed = new boolean[1];

        store.compute(key, (tuple, ts) -> {
            boolean st = ts != null ? ts.remove(rowId) : false;

            removed[0] = st;

            return ts;
        });

        return removed[0];
    }
}
