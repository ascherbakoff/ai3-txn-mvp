package com.ascherbakoff.ai3.table;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

interface HashIndex<T> {
    Iterator<T> scan(Tuple key);

    boolean insert(Tuple key, T rowId);

    boolean remove(Tuple key, T rowId);

    default Set<T> getAll(Tuple key) {
        Set<T> values = new HashSet<>();

        scan(key).forEachRemaining(values::add);

        return values;
    }
}
