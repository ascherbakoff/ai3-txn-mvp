package com.ascherbakoff.ai3.table;

import java.util.Map.Entry;
import org.jetbrains.annotations.Nullable;

interface SortedIndexStore<T> {
    Cursor<Entry<Tuple, Cursor<T>>> scan(@Nullable Tuple lower, boolean lowerInclusive, @Nullable Tuple upper, boolean upperInclusve);

    boolean insert(Tuple key, T rowId);

    boolean remove(Tuple key, T rowId);

    @Nullable Tuple nextKey(Tuple key);
}
