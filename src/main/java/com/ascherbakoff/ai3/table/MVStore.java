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

    // Run query in RW mode.
    AsyncCursor<Tuple> query(Query query, UUID txId);

    // Run query in RO mode.
    Cursor<Tuple> query(Query query, Timestamp readTs);

    // Commits a transaction with a timestamp.
    CompletableFuture<Boolean> commit(UUID txId, Timestamp commitTs);

    // Aborts a transaction
    CompletableFuture<Boolean> abort(UUID txId);
}
