package com.ascherbakoff.ai3.table;

import com.ascherbakoff.ai3.lock.Lock;
import com.ascherbakoff.ai3.lock.LockMode;
import com.ascherbakoff.ai3.lock.LockTable;
import com.ascherbakoff.ai3.table.MVStoreImpl.TxState;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class HashUniqueIndex extends HashNonUniqueIndex {
    public HashUniqueIndex(
            int col,
            LockTable lockTable,
            HashIndexStore<VersionChain<Tuple>> index,
            VersionChainRowStore<Tuple> rowStore
    ) {
        super(col, lockTable, index, rowStore);
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

                        // Read lock is not required here - we are protected from "dangerous" concurrent changes by index key lock.
                        if (rowStore.get(rowId0, txId, tuple -> (tuple == Tuple.TOMBSTONE ? Tuple.TOMBSTONE : tuple.select(col)).equals(newVal)) != null) {
                            throw new UniqueException("Failed to insert the row: duplicate index col=" + col + " key=" + newVal);
                        }
                    }

                    if (index.insert(newVal, rowId)) {
                        txState.addUndo(() -> index.remove(newVal, rowId));
                    }
                }));
            }
        }

        return CompletableFuture.allOf(futs.toArray(new CompletableFuture[0]));
    }
}
