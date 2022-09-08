package com.ascherbakoff.ai3.table;

import com.ascherbakoff.ai3.clock.Timestamp;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface MVStore {
    // Puts a row. This call updates primary and all secondary indexes after acquiring necessary locks.
    CompletableFuture<VersionChain<Tuple>> insert(Tuple row, UUID txId);

    CompletableFuture<Tuple> update(VersionChain<Tuple> rowId, Tuple newRow, UUID txId);

    // Removes a row. This call updates primary and all secondary indexes after acquiring necessary locks.
    CompletableFuture<Tuple> remove(VersionChain<Tuple> rowId, UUID txId);

    // Run query in RW mode.
    AsyncCursor<VersionChain<Tuple>> query(Query query, UUID txId);

    // Fetches a tuple by rowId.
    CompletableFuture<Tuple> get(VersionChain<Tuple> rowId, UUID txId);

    // Run query in RO mode.
    Cursor<Tuple> query(Query query, Timestamp readTs);

    // Commits a transaction with a timestamp.
    void commit(UUID txId, Timestamp commitTs);

    // Aborts a transaction
    void abort(UUID txId);
}
