package com.ascherbakoff.ai3.table;

import com.ascherbakoff.ai3.lock.Lock;
import com.ascherbakoff.ai3.lock.LockMode;
import com.ascherbakoff.ai3.lock.LockTable;
import com.ascherbakoff.ai3.table.MVStoreImpl.TxState;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class HashNonUniqueIndex implements Index {
    int col;
    LockTable lockTable;
    HashIndexStore<VersionChain<Tuple>> index;
    VersionChainRowStore<Tuple> rowStore;

    public HashNonUniqueIndex(
            int col,
            LockTable lockTable,
            HashIndexStore<VersionChain<Tuple>> index,
            VersionChainRowStore<Tuple> rowStore
    ) {
        this.col = col;
        this.lockTable = lockTable;
        this.index = index;
        this.rowStore = rowStore;
    }

    @Override
    public CompletableFuture update(UUID txId, TxState txState, Tuple oldRow, Tuple newRow, VersionChain<Tuple> rowId) {
        Tuple oldVal = oldRow == Tuple.TOMBSTONE ? Tuple.TOMBSTONE : oldRow.select(col);
        Tuple newVal = newRow == Tuple.TOMBSTONE ? Tuple.TOMBSTONE : newRow.select(col);

        List<CompletableFuture> futs = new ArrayList<>();

        if (!oldVal.equals(newVal)) {
            if (oldVal.length() > 0) {
                Lock lock0 = lockTable.getOrAddEntry(oldVal);

                txState.addLock(lock0);

                futs.add(lock0.acquire(txId, LockMode.IX));

                // Do not remove bookmarks due to multi-versioning.
            }

            if (newVal.length() > 0) {
                Lock lock0 = lockTable.getOrAddEntry(newVal);

                txState.addLock(lock0);

                futs.add(lock0.acquire(txId, LockMode.IX).thenAccept(ignored0 -> {
                    if (index.insert(newVal, rowId)) {
                        txState.addUndo(() -> index.remove(newVal, rowId));
                    }
                }));
            }
        }

        return CompletableFuture.allOf(futs.toArray(new CompletableFuture[0]));
    }

    @Override
    public AsyncCursor<VersionChain<Tuple>> eq(UUID txId, TxState txState, EqQuery query0) {
        return new AsyncCursor<VersionChain<Tuple>>() {
            Cursor<VersionChain<Tuple>> iter;

            @Override
            public CompletableFuture<VersionChain<Tuple>> nextAsync() {
                if (iter == null) {
                    Lock lock = lockTable.getOrAddEntry(query0.queryKey);

                    txState.addLock(lock);

                    return lock.acquire(txId, LockMode.S).thenApply(ignored -> {
                        iter = index.scan(query0.queryKey);

                        return iter.next();
                    });
                } else {
                    VersionChain<Tuple> tup = iter.next();

                    return CompletableFuture.completedFuture(tup);
                }
            }
        };
    }

    @Override
    public AsyncCursor<VersionChain<Tuple>> range(UUID txId, TxState txState, RangeQuery query0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Cursor<VersionChain<Tuple>> eq(EqQuery query0) {
        return index.scan(query0.queryKey);
    }
}
