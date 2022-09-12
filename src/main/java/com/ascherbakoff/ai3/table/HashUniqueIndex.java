package com.ascherbakoff.ai3.table;

import com.ascherbakoff.ai3.lock.Lock;
import com.ascherbakoff.ai3.lock.LockMode;
import com.ascherbakoff.ai3.lock.LockTable;
import com.ascherbakoff.ai3.table.MVStoreImpl.TxState;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public class HashUniqueIndex implements Index {
    int col;
    LockTable lockTable;
    HashIndexStore<VersionChain<Tuple>> index;
    VersionChainRowStore<Tuple> rowStore;

    public HashUniqueIndex(
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
    public CompletableFuture<Void> insert(UUID txId, TxState txState, Tuple row, VersionChain<Tuple> rowId) {
        Tuple newVal = row.select(col);

        Lock lock = lockTable.getOrAddEntry(newVal);

        txState.addLock(lock);

        return lock.acquire(txId, LockMode.X).thenAccept(ignored -> {
            Cursor<VersionChain<Tuple>> rowIds = index.scan(newVal);

            VersionChain<Tuple> rowId0;

            while((rowId0 = rowIds.next()) != null) {
                if (rowId0 == rowId) {
                    continue;
                }

                // TODO FIXME LOCK IS NEEDED - a concurrent tx can insert conflicting value and commit just after check.
                if (rowStore.get(rowId0, txId, tuple -> tuple.select(col).equals(newVal)) != null)
                    throw new UniqueException("Failed to insert the row: duplicate index col=" + col + " key=" + newVal);
            }

            if (index.insert(newVal, rowId)) {
                // Undo insertion only if this transactions inserts a new entry.
                txState.addUndo(() -> index.remove(newVal, rowId));
            }
        });
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

                futs.add(lock0.acquire(txId, LockMode.X));

                // Do not remove bookmarks due to multi-versioning.
            }

            // TODO FIXME remove copypaste.
            if (newVal.length() > 0) {
                Lock lock0 = lockTable.getOrAddEntry(newVal);

                txState.addLock(lock0);

                futs.add(lock0.acquire(txId, LockMode.X).thenAccept(ignored0 -> {
                    Cursor<VersionChain<Tuple>> rowIds = index.scan(newVal);

                    VersionChain<Tuple> rowId0;

                    while ((rowId0 = rowIds.next()) != null) {
                        if (rowId0 == rowId) {
                            continue;
                        }

                        // TODO FIXME LOCK IS NEEDED
                        if (rowStore.get(rowId0, txId, tuple -> tuple.select(col).equals(newVal)) != null) {
                            throw new UniqueException("Failed to insert the row: duplicate index col=" + col + " key=" + newVal);
                        }
                    }

                    if (index.insert(newVal, rowId)) {
                        txState.addUndo(() -> index.remove(newVal, rowId));
                    } else {
                        throw new UniqueException("Failed to insert the row: duplicate index col=" + col + " key=" + newVal);
                    }
                }));
            }
        }

        return CompletableFuture.allOf(futs.toArray(new CompletableFuture[0]));
    }

    @Override
    public CompletableFuture remove(UUID txId, TxState txState, Tuple removed, VersionChain<Tuple> rowId) {
        Tuple oldVal = removed.select(col);

        Lock lock = lockTable.getOrAddEntry(oldVal);

        txState.addLock(lock);

        // Do not physically remove bookmarks from the index due to multi-versioning.

        return lock.acquire(txId, LockMode.X);
    }

    @Override
    public AsyncCursor<VersionChain<Tuple>> eq(UUID txId, TxState txState, EqQuery query0) {
        AtomicReference<Cursor<VersionChain<Tuple>>> first = new AtomicReference<>();

        return new AsyncCursor<VersionChain<Tuple>>() {
            @Override
            public CompletableFuture<VersionChain<Tuple>> nextAsync() {
                Cursor<VersionChain<Tuple>> iter = first.get();
                if (iter == null) {
                    Lock lock = lockTable.getOrAddEntry(query0.queryKey);

                    txState.addLock(lock);

                    return lock.acquire(txId, LockMode.S).thenApply(ignored -> {
                        Cursor<VersionChain<Tuple>> iter0 = index.scan(query0.queryKey);

                        first.set(iter0);

                        return iter0.next();
                    });
                } else {
                    VersionChain<Tuple> tup = iter.next();

                    return CompletableFuture.completedFuture(tup);
                }
            }
        };
    }
}