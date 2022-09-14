package com.ascherbakoff.ai3.table;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.lock.Lock;
import com.ascherbakoff.ai3.lock.LockMode;
import com.ascherbakoff.ai3.lock.LockTable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/**
 * TODO reduce copypaste, compact tx state
 */
public class MVStoreImpl implements MVStore {
    private final VersionChainRowStore<Tuple> rowStore;
    private final LockTable lockTable;
    private Map<Integer, Index> indexes;

    Map<UUID, TxState> txnLocalMap = new ConcurrentHashMap<>(); // TBD add size overflow test

    final int idxCnt;

    public MVStoreImpl(VersionChainRowStore<Tuple> rowStore, LockTable lockTable, Map<Integer, Index> indexes) {
        this.indexes = indexes;
        this.lockTable = lockTable;
        this.rowStore = rowStore;
        this.idxCnt = indexes.size();
    }

    @Override
    public CompletableFuture<VersionChain<Tuple>> insert(Tuple row, UUID txId) {
        TxState txState = localState(txId);

        VersionChain<Tuple> rowId = rowStore.insert(row, txId);

        txState.addWrite(rowId);

        List<CompletableFuture> futs = new ArrayList<>(idxCnt);

        // TODO FIXME lock sorting ??? Can we get deadlocks on index updates ?.
        for (Entry<Integer, Index> entry : indexes.entrySet()) {
            futs.add(entry.getValue().update(txId, txState, Tuple.TOMBSTONE, row, rowId));
        }

        return CompletableFuture.allOf(futs.toArray(new CompletableFuture[0])).thenApply(ignored -> rowId);
    }

    @Override
    public CompletableFuture<Tuple> update(VersionChain<Tuple> rowId, Tuple newRow, UUID txId) {
        Lock lock = lockTable.getOrAddEntry(rowId);

        TxState txState = localState(txId);

        txState.addLock(lock);

        return lock.acquire(txId, LockMode.X).thenCompose(ignored -> {
            Tuple oldRow = rowStore.update(rowId, newRow, txId);

            txState.addWrite(rowId);

            List<CompletableFuture> futs = new ArrayList<>(idxCnt);

            for (Entry<Integer, Index> entry : indexes.entrySet()) {
                futs.add(entry.getValue().update(txId, txState, oldRow, newRow, rowId));
            }

            return CompletableFuture.allOf(futs.toArray(new CompletableFuture[0])).thenApply(ignored0 -> oldRow);
        });
    }

    @Override
    public CompletableFuture<Tuple> remove(VersionChain<Tuple> rowId, UUID txId) {
        return update(rowId, Tuple.TOMBSTONE, txId);
    }

    @Override
    public CompletableFuture<Tuple> get(VersionChain<Tuple> rowId, UUID txId, Predicate<Tuple> filter) {
        Lock lock = lockTable.getOrAddEntry(rowId);

        TxState txState = localState(txId);

        txState.addLock(lock);

        return lock.acquire(txId, LockMode.S).thenApply(x -> rowId.resolve(txId, null, filter));
    }

    @Override
    public AsyncCursor<VersionChain<Tuple>> query(Query query, UUID txId) {
        TxState txState = localState(txId);
        // TODO FIXME remove instanceof
        if (query instanceof ScanQuery) {
            // TODO FIXME table lock

            Cursor<VersionChain<Tuple>> cur = rowStore.scan(txId);

            return new AsyncCursor<VersionChain<Tuple>>() {
                @Override
                public CompletableFuture<VersionChain<Tuple>> nextAsync() {
                    VersionChain<Tuple> rowId = cur.next();

                    return CompletableFuture.completedFuture(rowId);
                }
            };
        } else if (query instanceof EqQuery) {
            EqQuery query0 = (EqQuery) query;

            Index idx = indexes.get(query0.col);

            if (idx == null) {
                throw new IllegalArgumentException("Hash index not found for col=" + query0.col);
            }

            return idx.eq(txId, txState, query0);
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

        if (state == null) {
            return; // Already finished.
        }

        for (VersionChain<Tuple> write : state.writes) {
            rowStore.commitWrite(write, commitTs, txId);
        }

        for (Lock lock : state.locks) {
            lock.release(txId);
        }
    }

    @Override
    public void abort(UUID txId) {
        TxState state = txnLocalMap.remove(txId);

        if (state == null) {
            return; // Already finished.
        }

        for (VersionChain<Tuple> write : state.writes) {
            rowStore.abortWrite(write, txId);
        }

        for (Runnable undo : state.undos) {
            undo.run();
        }

        for (Lock lock : state.locks) {
            lock.release(txId);
        }
    }

    private TxState localState(UUID txId) {
        return txnLocalMap.computeIfAbsent(txId, k -> new TxState());
    }

    static class TxState {
        Set<Lock> locks = new HashSet<>();
        Set<VersionChain<Tuple>> writes = new HashSet<>();
        Set<Runnable> undos = new HashSet<>();

        synchronized void addLock(Lock lock) {
            locks.add(lock);
        }

        synchronized void addWrite(VersionChain<Tuple> rowId) {
            writes.add(rowId);
        }

        public void addUndo(Runnable r) {
            undos.add(r);
        }
    }
}
