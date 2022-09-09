package com.ascherbakoff.ai3.table;

import com.ascherbakoff.ai3.lock.LockTable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
class HashIndexImpl<T> implements HashIndex<T> {
    private final LockTable lockTable;
    ConcurrentMap<Tuple, Set<T>> data = new ConcurrentHashMap<>();

    HashIndexImpl(LockTable lockTable) {
        this.lockTable = lockTable;
    }

    @Override
    public Cursor<T> scan(Tuple key) {
        Set<T> vals = data.get(key);

        if (vals == null) {
            vals = Collections.emptySet();
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
                ts = new HashSet<>();
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
            boolean st = ts.remove(rowId);

            removed[0] = st;

            return ts;
        });

        return removed[0];
    }

    @Override
    public LockTable lockTable() {
        return lockTable;
    }
}
