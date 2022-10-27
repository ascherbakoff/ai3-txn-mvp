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
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;

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

                Lock nextLock = lockTable.getOrAddEntry(nextKey);

                txState.addLock(nextLock);

                Locker locker = nextLock.acquire(txId, LockMode.IX);

                futs.add(locker.thenCompose(prevMode -> {
                    Lock curLock = lockTable.getOrAddEntry(newVal);

                    txState.addLock(curLock);

                    LockMode lockedInMode = locker.getMode();

                    LockMode mode = lockedInMode == LockMode.S || lockedInMode == LockMode.X || lockedInMode == LockMode.SIX ? LockMode.X : LockMode.IX;

                    return curLock.acquire(txId, mode).thenAccept(ignored -> {
                        if (index.insert(newVal, rowId)) {
                            txState.addUndo(() -> index.remove(newVal, rowId));
                        }

                        if (prevMode != null) {
                            if (prevMode != LockMode.IX) { // Lock was upgraded.
                                LockMode mode0 = nextLock.downgrade(txId, prevMode);
                            } // Otherwise do nothing.
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

    @Override
    public AsyncCursor<VersionChain<Tuple>> eq(UUID txId, TxState txState, EqQuery query0) {
        return range(txId, txState, new RangeQuery(query0));
    }

    @Override
    public AsyncCursor<VersionChain<Tuple>> range(UUID txId, TxState txState, RangeQuery query0) {
        Cursor<Entry<Tuple, Cursor<VersionChain<Tuple>>>> cur = index
                .scan(query0.lowerKey, query0.lowerInclusive, query0.upperKey, query0.upperInclusive);

        return new AsyncCursor<VersionChain<Tuple>>() {
            Cursor<VersionChain<Tuple>> rowIter;

            @Override
            public CompletableFuture<VersionChain<Tuple>> nextAsync() {
                return getNext(query0, txId, txState, cur);
            }

            private CompletableFuture<VersionChain<Tuple>> getNext(
                    RangeQuery query0, UUID txId,
                    TxState txState,
                    Cursor<Entry<Tuple, Cursor<VersionChain<Tuple>>>> iter
            ) {
                if (rowIter != null) {
                    VersionChain<Tuple> tup = rowIter.next();

                    if (tup != null)
                        return CompletableFuture.completedFuture(tup);
                }

                // Optimistically read next entry
                Entry<Tuple, Cursor<VersionChain<Tuple>>> next = iter.next();

                Tuple lockKey = next == null ? query0.upperKey == null ? Tuple.INF : query0.upperKey : next.getKey();

                if (query0.delayOnNext != null) {
                    try {
                        query0.delayOnNext.await();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                Lock lock = lockTable.getOrAddEntry(lockKey);

                txState.addLock(lock);

                return lock.acquire(txId, LockMode.S).thenComposeAsync(lockMode -> { // Avoid recursion.
                    if (next == null)
                        return CompletableFuture.completedFuture(null);

                    // Re-check after lock acquisition.
                    if (index.contains(lockKey)) {
                        rowIter = next.getValue();

                        return getNext(query0, txId, txState, iter);
                    } else {
                        return getNext(query0, txId, txState, iter);
                    }
                });
            }
        };
    }
}
