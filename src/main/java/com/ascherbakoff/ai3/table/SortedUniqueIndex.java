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
import org.jetbrains.annotations.Nullable;

// TODO FIXME remove copy paste
public class SortedUniqueIndex implements Index {
    int col;
    LockTable lockTable;
    SortedIndexStore<VersionChain<Tuple>> index;
    VersionChainRowStore<Tuple> rowStore;

    public SortedUniqueIndex(
            int col,
            LockTable lockTable,
            SortedIndexStore<VersionChain<Tuple>> index,
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

                futs.add(lock0.acquire(txId, LockMode.X));

                // Do not remove bookmarks due to multi-versioning.
            }

            if (newVal.length() > 0) {
                Tuple nextKey = index.nextKey(newVal);

                if (nextKey == null) {
                    nextKey = Tuple.INF;
                }

                Lock lock0 = lockTable.getOrAddEntry(nextKey);

                txState.addLock(lock0);

                Locker locker = lock0.acquire(txId, LockMode.IX);

                futs.add(locker.thenCompose(lockMode -> {
                    Lock lock1 = lockTable.getOrAddEntry(newVal);

                    txState.addLock(lock1);

                    return lock1.acquire(txId, LockMode.X).thenAccept(ignored -> {
                        if (index.insert(newVal, rowId)) {
                            txState.addUndo(() -> index.remove(newVal, rowId));
                        }

                        Cursor<Entry<Tuple, Cursor<VersionChain<Tuple>>>> cur = index.scan(newVal, true, newVal, true);

                        Entry<Tuple, Cursor<VersionChain<Tuple>>> next0 = cur.next();

                        assert next0 != null && next0.getKey().equals(newVal);

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

                        if (lockMode != null && lockMode != LockMode.IX) { // Lock was upgraded.
                            LockMode mode = lock0.downgrade(txId, lockMode);

                            assert lockMode == mode : lockMode + "->" + mode;
                        } else {
                            lock0.release(txId);
                            txState.locks.remove(lock0);
                        }
                    });
                }));
            }
        }

        return CompletableFuture.allOf(futs.toArray(new CompletableFuture[0]));
    }

    @Override
    public AsyncCursor<VersionChain<Tuple>> eq(UUID txId, TxState txState, EqQuery query0) {
        return range(txId, txState, new RangeQuery(query0));
    }

    @Override
    public AsyncCursor<VersionChain<Tuple>> range(UUID txId, TxState txState, RangeQuery query0) {
        Cursor<Entry<Tuple, Cursor<VersionChain<Tuple>>>> cur = index
                .scan(query0.lowerKey, query0.lowerInclusive, query0.upperKey, query0.upperInclusive);

        return new AsyncCursor<VersionChain<Tuple>>() {
            @Override
            public CompletableFuture<VersionChain<Tuple>> nextAsync() {
                return getNext(query0, txId, txState, cur, null);
            }
        };
    }

    private CompletableFuture<VersionChain<Tuple>> getNext(
            RangeQuery query0, UUID txId,
            TxState txState,
            Cursor<Entry<Tuple, Cursor<VersionChain<Tuple>>>> iter,
            @Nullable Cursor<VersionChain<Tuple>> iter0
    ) {
        if (iter0 != null) {
            VersionChain<Tuple> tup = iter0.next();

            if (tup != null)
                return CompletableFuture.completedFuture(tup);
        }

        // Optimistically read next entry
        Entry<Tuple, Cursor<VersionChain<Tuple>>> next = iter.next();

        Tuple lockKey = next == null ? query0.upperKey == null ? Tuple.INF : query0.upperKey : next.getKey();

        Lock lock = lockTable.getOrAddEntry(lockKey);

        txState.addLock(lock);

        return lock.acquire(txId, LockMode.S).thenComposeAsync(lockMode -> { // Avoid recursion.
            if (next == null)
                return CompletableFuture.completedFuture(null);

            // Re-check after lock acquisition.
            if (index.contains(lockKey)) {
                Cursor<VersionChain<Tuple>> value = next.getValue();

                return getNext(query0, txId, txState, iter, value);
            } else {
                return getNext(query0, txId, txState, iter, null);
            }
        });
    }
}
