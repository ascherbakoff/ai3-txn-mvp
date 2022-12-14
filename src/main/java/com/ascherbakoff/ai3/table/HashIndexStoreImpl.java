package com.ascherbakoff.ai3.table;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
class HashIndexStoreImpl<T> implements HashIndexStore<T> {
    ConcurrentMap<Tuple, Set<T>> data = new ConcurrentHashMap<>();

    @Override
    public Cursor<T> scan(Tuple key) {
        Set<T> vals = data.get(key);

        if (vals == null) {
            return Cursor.EMPTY;
        }

        Iterator<T> iter = vals.iterator();

        return new Cursor<T>() {
            @Override
            public T next() {
                if (!iter.hasNext()) {
                    return null;
                }

                return iter.next();
            }
        };
    }

    @Override
    public boolean insert(Tuple key, T rowId) {
        final boolean[] inserted = new boolean[1];

        data.compute(key, (tuple, ts) -> {
            if (ts == null) {
                ts = Collections.newSetFromMap(new ConcurrentHashMap<>());
            }

            inserted[0] = ts.add(rowId);

            return ts;
        });

        return inserted[0];
    }

    @Override
    public boolean remove(Tuple key, T rowId) {
        final boolean[] removed = new boolean[1];

        data.compute(key, (tuple, ts) -> {
            boolean st = ts != null ? ts.remove(rowId) : false;

            removed[0] = st;

            return st ? ts.isEmpty() ? null : ts : ts;
        });

        return removed[0];
    }
}
