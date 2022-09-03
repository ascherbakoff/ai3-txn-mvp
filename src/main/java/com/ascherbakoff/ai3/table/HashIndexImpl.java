package com.ascherbakoff.ai3.table;

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
        // Must validate uniquess before insert.
        if (unique) {
            Set<T> values = data.get(key);

            if (values != null && !values.contains(rowId))
                return false;
        }

        data.computeIfAbsent(key, k -> new HashSet<>()).add(rowId);

        return true;
    }

    @Override
    public boolean remove(Tuple key, T rowId) {
        Set<T> values = data.get(key);

        return values.remove(rowId);
    }
}
