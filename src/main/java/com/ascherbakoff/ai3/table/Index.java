package com.ascherbakoff.ai3.table;

import com.ascherbakoff.ai3.table.MVStoreImpl.TxState;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface Index {
    CompletableFuture update(UUID txId, TxState txState, Tuple oldRow, Tuple newRow, VersionChain<Tuple> rowId);

    AsyncCursor<VersionChain<Tuple>> eq(UUID txId, TxState txState, EqQuery query0);

    AsyncCursor<Entry<Tuple, Cursor<VersionChain<Tuple>>>> range(UUID txId, TxState txState, RangeQuery query0);
}
