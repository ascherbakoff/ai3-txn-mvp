package com.ascherbakoff.ai3.table;

import com.ascherbakoff.ai3.table.MVStoreImpl.TxState;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface Index {
    CompletableFuture<Void> insert(UUID txId, TxState txState, Tuple row, VersionChain<Tuple> rowId);

    CompletableFuture update(UUID txId, TxState txState, Tuple oldRow, Tuple newRow, VersionChain<Tuple> rowId);

    CompletableFuture remove(UUID txId, TxState txState, Tuple removed, VersionChain<Tuple> rowId);

    AsyncCursor<VersionChain<Tuple>> eq(UUID txId, TxState txState, EqQuery query0);
}
