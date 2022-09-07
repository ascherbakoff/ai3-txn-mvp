package com.ascherbakoff.ai3.table;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.lock.DeadlockPrevention;
import com.ascherbakoff.ai3.lock.Lock;
import com.ascherbakoff.ai3.lock.LockMode;
import com.ascherbakoff.ai3.lock.LockTable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class MVStoreImpl implements MVStore {
    private final VersionChainRowStore<Tuple> rowStore;
    private LockTable lockTable;
    private int[][] indexes;
    private HashIndex<VersionChain<Tuple>> primaryIndex = new HashIndexImpl<>(true);
    Map<UUID, TxState> txnLocalMap = new ConcurrentHashMap<>(); // TBD add overflow test

    public MVStoreImpl(int[][] indexes) {
        assert indexes.length > 0 : "PK index is mandatory";

        this.lockTable = new LockTable(10, true, DeadlockPrevention.none());
        this.indexes = indexes;
        this.rowStore = new VersionChainRowStore<Tuple>();
    }

    private TxState localState(UUID txId) {
        return txnLocalMap.computeIfAbsent(txId, k -> new TxState());
    }

    @Override
    public CompletableFuture<Void> insert(Tuple row, UUID txId) {
        Tuple pk = row.select(indexes[0]);

        TxState txState = localState(txId);

        Lock lock = lockTable.getOrAddEntry(pk);

        txState.addLock(lock);

        return lock.acquire(txId, LockMode.X).thenAccept(ignored -> {
            // Inserted rowId is guaranteed to be unique.
            VersionChain<Tuple> rowId = rowStore.insert(row, txId);

            txState.addWrite(rowId);

            if (!primaryIndex.insert(pk, rowId)) {
                throw new UniqueException("Failed to insert the row: duplicate primary key " + pk);
            }

            // TODO insert into other indexes
        });
    }

    @Override
    public CompletableFuture<Tuple> remove(Tuple keyTuple, UUID txId) {
        return null;
    }

    @Override
    public AsyncCursor<Tuple> query(Query query, UUID txId) {
        if (query instanceof ScanQuery) {
            Cursor<Tuple> cur = rowStore.scan(txId);

            return new AsyncCursor<Tuple>() {
                @Override
                public CompletableFuture<Tuple> nextAsync() {
                    Tuple tup = cur.next();

                    if (tup == null)
                        return CompletableFuture.completedFuture(null);

                    Lock lock = lockTable.getOrAddEntry(tup);

                    return lock.acquire(txId, LockMode.S).thenApplyAsync(x -> tup);
                }
            };
        }

        return null;
    }

    @Override
    public Cursor<Tuple> query(Query query, Timestamp readTs) {
        return null;
    }

    @Override
    public void commit(UUID txId, Timestamp commitTs) {
        TxState state = txnLocalMap.remove(txId);

        if (state == null)
            return; // Already finished.

        for (VersionChain<Tuple> write : state.writes) {
            rowStore.commitWrite(write, commitTs, txId);
        }

        for (Lock lock : state.locks) {
            lock.release(txId);
        }

        txnLocalMap.remove(txId);
    }

    @Override
    public void abort(UUID txId) {
        TxState state = txnLocalMap.remove(txId);

        if (state == null)
            return; // Already finished.

        for (VersionChain<Tuple> write : state.writes) {
            rowStore.abortWrite(write, txId);
        }

        for (Lock lock : state.locks) {
            lock.release(txId);
        }
    }

    static class TxState {
        Set<Lock> locks = new HashSet<>();
        Set<VersionChain<Tuple>> writes = new HashSet<>();

        synchronized void addLock(Lock lock) {
            locks.add(lock);
        }

        synchronized void addWrite(VersionChain<Tuple> rowId) {
            writes.add(rowId);
        }
    }
}
