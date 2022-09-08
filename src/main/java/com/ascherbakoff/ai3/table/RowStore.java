package com.ascherbakoff.ai3.table;

import com.ascherbakoff.ai3.clock.Timestamp;
import java.util.UUID;
import java.util.function.Predicate;
import org.jetbrains.annotations.Nullable;

interface RowStore<RowID, T> {
    // TODO do we need 3 arg ?
    T get(RowID rowId, UUID txId, @Nullable Predicate<T> filter);
    T get(RowID rowId, @Nullable Timestamp timestamp, @Nullable Predicate<T> filter);
    RowID insert(T newRow, UUID txId);
    void update(RowID rowId, @Nullable T newRow, UUID txId);
    boolean remove(RowID rowId);
    void commitWrite(RowID rowId, Timestamp timestamp, UUID txId);
    void abortWrite(RowID rowId, UUID txId);
    Cursor<T> scan(UUID txId);
    Cursor<T> scan(Timestamp timestamp);
}