package com.ascherbakoff.ai3.table;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

/**
 *
 */
class HashIndexImpl<T> implements HashIndex<T> {
    final boolean unique;
    ConcurrentMap<Tuple, Set<T>> data = new ConcurrentHashMap<>();

    HashIndexImpl(boolean unique) {
        this.unique = unique;
    }

    @Override
    public Iterator<T> scan(Tuple key) {
        Set<T> vals = data.get(key);

        if (vals == null)
            vals = Collections.emptySet();

        return vals.iterator();
    }

    @Override
    public boolean insert(Tuple key, T rowId) {
        try {
            data.compute(key, new BiFunction<Tuple, Set<T>, Set<T>>() {
                @Override
                public Set<T> apply(Tuple tuple, Set<T> ts) {
                    if (unique) {
                        if (ts != null && !ts.contains(rowId))
                            throw new RuntimeException("Unique violation " + tuple);
                    }

                    if (ts == null)
                        ts = new HashSet<>();

                    ts.add(rowId);

                    return ts;
                }
            });
        } catch (Exception e) {
            return false;
        }

        return true;
    }

    @Override
    public boolean remove(Tuple key, T rowId) {
        final boolean[] removed = new boolean[1];

        data.compute(key, new BiFunction<Tuple, Set<T>, Set<T>>() {
            @Override
            public Set<T> apply(Tuple tuple, Set<T> ts) {
                boolean st = ts.remove(rowId);

                removed[0] = st;

                return ts;
            }
        });

        return removed[0];
    }
}
