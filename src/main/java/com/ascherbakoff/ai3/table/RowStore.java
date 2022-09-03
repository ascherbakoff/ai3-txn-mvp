package com.ascherbakoff.ai3.table;

import com.ascherbakoff.ai3.clock.Timestamp;
import java.util.UUID;
import java.util.function.Predicate;
import org.jetbrains.annotations.Nullable;

interface RowStore<RowID, T> {
    T get(RowID rowId, UUID txId, Predicate<T> filter);
    T get(RowID rowId, @Nullable Timestamp timestamp, Predicate<T> filter);
    T getForUpdate(RowID rowId, UUID txId, Predicate<T> filter);
    RowID insert(T newRow, UUID txId);
    void update(RowID rowId, T newRow, UUID txId);
    boolean remove(RowID rowId);
    void commitWrite(RowID rowId, Timestamp timestamp, UUID txId);
    void abortWrite(RowID rowId, UUID txId);
    Cursor<T> scan(UUID txId);
    Cursor<T> scan(Timestamp timestamp);
}
