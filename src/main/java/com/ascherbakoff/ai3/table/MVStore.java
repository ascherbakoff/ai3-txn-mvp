package com.ascherbakoff.ai3.table;

import com.ascherbakoff.ai3.clock.Timestamp;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import org.jetbrains.annotations.Nullable;

public interface MVStore {
    // Puts a row. This call updates primary and all secondary indexes after acquiring necessary locks.
    CompletableFuture<Void> put(Tuple row, UUID txId);

    // Removes a row. This call updates primary and all secondary indexes after acquiring necessary locks.
    CompletableFuture<Tuple> remove(Tuple keyTuple, UUID txId);

    // Scans primary or secondary index in RW mode. This call acquires necessary locks for the read.
// If a filter is null, a full scan is assumed.
    Cursor<Tuple> scan(String idxName, @Nullable Predicate<Tuple> filter, UUID txId);

    // Scans primary or secondary index in snapshot mode.
// If a filter is null, a full scan is assumed.
// If the index type doesn’t allow the execution of a range scan, it falls back to a full scan.
    Cursor<Tuple> scan(String idxName, @Nullable Predicate<Tuple> filter, Timestamp readTs);

    // Commits a transaction with a timestamp.
    CompletableFuture<Boolean> commit(UUID txId, Timestamp commitTs);

    // Aborts a transaction
    CompletableFuture<Boolean> abort(UUID txId);
}
