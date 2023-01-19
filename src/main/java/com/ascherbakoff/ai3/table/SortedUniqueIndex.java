package com.ascherbakoff.ai3.table;

import com.ascherbakoff.ai3.lock.Lock;
import com.ascherbakoff.ai3.lock.LockMode;
import com.ascherbakoff.ai3.lock.LockTable;
import com.ascherbakoff.ai3.lock.Locker;
import com.ascherbakoff.ai3.table.MVStoreImpl.TxState;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class SortedUniqueIndex extends SortedNonUniqueIndex {
    public SortedUniqueIndex(
            int col,
            LockTable lockTable,
            SortedIndexStore<VersionChain<Tuple>> index,
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
                Tuple nextKey = index.nextKey(newVal);

                if (nextKey == null) {
                    nextKey = Tuple.INF;
                }

                Lock nextLock = lockTable.getOrAddEntry(nextKey);

                txState.addLock(nextLock);

                Locker locker = nextLock.acquire(txId, LockMode.IX);

                futs.add(locker.thenCompose(prevMode -> {
                    Lock curLock = lockTable.getOrAddEntry(newVal);

                    txState.addLock(curLock);

                    return curLock.acquire(txId, LockMode.X).thenAccept(ignored -> {
                        Cursor<Entry<Tuple, Cursor<VersionChain<Tuple>>>> cur = index.scan(newVal, true, newVal, true);

                        Entry<Tuple, Cursor<VersionChain<Tuple>>> next0 = cur.next();

                        if (next0 != null) {
                            Cursor<VersionChain<Tuple>> rowIds = next0.getValue();

                            VersionChain<Tuple> rowId0;

                            while ((rowId0 = rowIds.next()) != null) {
                                if (rowId0 == rowId) {
                                    continue;
                                }

                                // Read lock is not required here - we are protected from "dangerous" concurrent changes by index key lock.
                                if (rowStore.get(rowId0, txId,
                                        tuple -> (tuple == Tuple.TOMBSTONE ? Tuple.TOMBSTONE : tuple.select(col)).equals(newVal)) != null) {
                                    throw new UniqueException("Failed to insert the row: duplicate index col=" + col + " key=" + newVal);
                                }
                            }
                        }

                        if (index.insert(newVal, rowId)) {
                            txState.addUndo(() -> index.remove(newVal, rowId));
                        }

                        if (prevMode != null) { // Lock was upgraded.
                            nextLock.downgrade(txId, LockMode.IX, prevMode);
                        } else {
                            nextLock.release(txId);
                            txState.locks.remove(nextLock);
                        }
                    });
                }));
            }
        }

        return CompletableFuture.allOf(futs.toArray(new CompletableFuture[0]));
    }
}
