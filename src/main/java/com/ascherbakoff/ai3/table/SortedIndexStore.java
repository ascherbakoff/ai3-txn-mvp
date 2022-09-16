package com.ascherbakoff.ai3.table;

import java.util.Map;
import org.jetbrains.annotations.Nullable;

interface SortedIndexStore<T> {
    Cursor<Map<Tuple, Cursor<T>>> scan(@Nullable Tuple lower, boolean lowerInclusive, @Nullable Tuple upper, boolean upperInclusve);

    boolean insert(Tuple key, T rowId);

    boolean remove(Tuple key, T rowId);
}
