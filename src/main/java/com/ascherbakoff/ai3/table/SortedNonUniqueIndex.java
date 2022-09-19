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
import java.util.concurrent.atomic.AtomicReference;

public class SortedNonUniqueIndex implements Index {
    int col;
    LockTable lockTable;
    SortedIndexStore<VersionChain<Tuple>> index;
    VersionChainRowStore<Tuple> rowStore;

    public SortedNonUniqueIndex(
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

                futs.add(lock0.acquire(txId, LockMode.IX));

                // Do not remove bookmarks due to multi-versioning.
            }

            if (newVal.length() > 0) {
                Tuple nextKey = index.nextKey(newVal);

                if (nextKey == null)
                    nextKey = Tuple.INF;

                Lock lock0 = lockTable.getOrAddEntry(nextKey);

                txState.addLock(lock0);

                Locker locker = lock0.acquire(txId, LockMode.IX);

                futs.add(locker.thenCompose(lockMode -> {
                    if (index.insert(newVal, rowId)) {
                        txState.addUndo(() -> index.remove(newVal, rowId));
                    }

                    if (lockMode != null && lockMode != LockMode.IX) { // Lock was upgraded.
                        LockMode mode = lock0.downgrade(txId, lockMode);

                        assert lockMode == mode : lockMode + "->" + mode;
                    }
                    else {
                        lock0.release(txId);
                        txState.locks.remove(lock0);
                    }

                    Lock lock1 = lockTable.getOrAddEntry(newVal);

                    txState.addLock(lock1);

                    LockMode mode = lockMode == LockMode.S || lockMode == LockMode.X || lockMode == LockMode.SIX ? LockMode.X : LockMode.IX;

                    return lock1.acquire(txId, mode);
                }));
            }
        }

        return CompletableFuture.allOf(futs.toArray(new CompletableFuture[0]));
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
                        Cursor<Entry<Tuple, Cursor<VersionChain<Tuple>>>> cur = index.scan(query0.queryKey, true, query0.queryKey, true);

                        Entry<Tuple, Cursor<VersionChain<Tuple>>> next = cur.next();

                        Cursor<VersionChain<Tuple>> cur0 = Cursor.EMPTY;

                        if (next != null) {
                            assert next.getKey().equals(query0.queryKey);

                            cur0 = next.getValue();
                        }

                        first.set(cur0);

                        return cur0.next();
                    });
                } else {
                    VersionChain<Tuple> tup = iter.next();

                    return CompletableFuture.completedFuture(tup);
                }
            }
        };
    }

    @Override
    public AsyncCursor<Entry<Tuple, Cursor<VersionChain<Tuple>>>> range(UUID txId, TxState txState, RangeQuery query0) {
        Cursor<Entry<Tuple, Cursor<VersionChain<Tuple>>>> cur = index.scan(query0.lowerKey, query0.lowerInclusive, query0.upperKey, query0.upperInclusive);

        return new AsyncCursor<Entry<Tuple, Cursor<VersionChain<Tuple>>>>() {
            @Override
            public CompletableFuture<Entry<Tuple, Cursor<VersionChain<Tuple>>>> nextAsync() {
                Cursor<Entry<Tuple, Cursor<VersionChain<Tuple>>>> iter = cur;

                Entry<Tuple, Cursor<VersionChain<Tuple>>> next = iter.next();

                Tuple lockKey = next == null ? Tuple.INF : next.getKey();

                Lock lock = lockTable.getOrAddEntry(lockKey);

                txState.addLock(lock);

                return lock.acquire(txId, LockMode.S).thenApply(ignored -> next);
            }
        };
    }
}
