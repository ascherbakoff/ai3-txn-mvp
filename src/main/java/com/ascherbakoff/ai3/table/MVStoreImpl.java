package com.ascherbakoff.ai3.table;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.lock.Lock;
import com.ascherbakoff.ai3.lock.LockMode;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TODO reduce copypaste, compact tx state
 */
public class MVStoreImpl implements MVStore {
    private final VersionChainRowStore<Tuple> rowStore;
    private Map<Integer, HashIndex<VersionChain<Tuple>>> hashIndexes;
    private Map<Integer, SortedIndex<VersionChain<Tuple>>> sortedIndexes;

    Map<UUID, TxState> txnLocalMap = new ConcurrentHashMap<>(); // TBD add size overflow test

    public MVStoreImpl(
            VersionChainRowStore<Tuple> rowStore,
            Map<Integer, HashIndex<VersionChain<Tuple>>> hashIndexes,
            Map<Integer, SortedIndex<VersionChain<Tuple>>> sortedIndexes
    ) {
        this.hashIndexes = hashIndexes;
        this.sortedIndexes = sortedIndexes;
        this.rowStore = rowStore;
    }

    @Override
    public CompletableFuture<Void> insert(Tuple row, UUID txId) {
        TxState txState = localState(txId);

        VersionChain<Tuple> rowId = rowStore.insert(row, txId);

        txState.addWrite(rowId);

        List<CompletableFuture> futs = new ArrayList<>(hashIndexes.size() + sortedIndexes.size());

        for (Entry<Integer, HashIndex<VersionChain<Tuple>>> entry : hashIndexes.entrySet()) {
            int col = entry.getKey();

            Tuple indexedKey = row.select(col);

            Lock lock = entry.getValue().lockTable().getOrAddEntry(indexedKey);

            txState.addLock(lock);

            futs.add(lock.acquire(txId, LockMode.X).thenAccept(ignored -> {
                // Inserted rowId is guaranteed to be unique, so no lock.
                if (!entry.getValue().insert(indexedKey, rowId)) {
                    throw new UniqueException("Failed to insert the row: duplicate primary key " + indexedKey);
                } else {
                    txState.addUndo(() -> entry.getValue().remove(indexedKey, rowId));
                }
            }));
        }

        return CompletableFuture.allOf(futs.toArray(new CompletableFuture[0]));
    }

    @Override
    public CompletableFuture<Tuple> remove(Tuple keyTuple, UUID txId) {
        return null;
    }

    @Override
    public AsyncCursor<Tuple> query(Query query, UUID txId) {
        TxState txState = localState(txId);
        // TODO FIXME remove instanceof
        if (query instanceof ScanQuery) {
            Cursor<Tuple> cur = rowStore.scan(txId);

            return new AsyncCursor<Tuple>() {
                @Override
                public CompletableFuture<Tuple> nextAsync() {
                    Tuple tup = cur.next();

                    if (tup == null)
                        return CompletableFuture.completedFuture(null);

                    Lock lock = rowStore.lockTable().getOrAddEntry(tup);

                    txState.addLock(lock);

                    return lock.acquire(txId, LockMode.S).thenApply(x -> tup);
                }
            };
        }
        else if (query instanceof EqQuery) {
            EqQuery query0 = (EqQuery) query;

            HashIndex<VersionChain<Tuple>> idx = hashIndexes.get(query0.col);

            if (idx == null)
                throw new IllegalArgumentException("Hash index not found for col=" + query0.col);

            Cursor<VersionChain<Tuple>> iter = idx.scan(query0.queryKey);

            return new AsyncCursor<Tuple>() {
                @Override
                public CompletableFuture<Tuple> nextAsync() {
                    while(true) {
                        VersionChain<Tuple> tup = iter.next();

                        if (tup == null)
                            return CompletableFuture.completedFuture(null);

                        Tuple val = tup.resolve(txId, null, null);

                        if (val == null)
                            continue;

                        Lock lock = rowStore.lockTable().getOrAddEntry(tup);

                        txState.addLock(lock);

                        return lock.acquire(txId, LockMode.S).thenApply(x -> val);
                    }
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
    }

    @Override
    public void abort(UUID txId) {
        TxState state = txnLocalMap.remove(txId);

        if (state == null)
            return; // Already finished.

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