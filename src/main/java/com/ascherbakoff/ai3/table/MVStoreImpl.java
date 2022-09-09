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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * TODO reduce copypaste, compact tx state
 */
public class MVStoreImpl implements MVStore {
    private final VersionChainRowStore<Tuple> rowStore;
    private Map<Integer, HashIndex<VersionChain<Tuple>>> hashUniqIndexes;
    private Map<Integer, HashIndex<VersionChain<Tuple>>> hashIndexes;
    private Map<Integer, SortedIndex<VersionChain<Tuple>>> sortedUniqIndexes;
    private Map<Integer, SortedIndex<VersionChain<Tuple>>> sortedIndexes;

    Map<UUID, TxState> txnLocalMap = new ConcurrentHashMap<>(); // TBD add size overflow test

    final int idxCnt;

    public MVStoreImpl(
            VersionChainRowStore<Tuple> rowStore,
            Map<Integer, HashIndex<VersionChain<Tuple>>> hashUniqIndexes,
            Map<Integer, HashIndex<VersionChain<Tuple>>> hashIndexes,
            Map<Integer, SortedIndex<VersionChain<Tuple>>> sortedUniqIndexes,
            Map<Integer, SortedIndex<VersionChain<Tuple>>> sortedIndexes
    ) {
        this.hashUniqIndexes = hashUniqIndexes;
        this.hashIndexes = hashIndexes;
        this.sortedUniqIndexes = sortedUniqIndexes;
        this.sortedIndexes = sortedIndexes;
        this.rowStore = rowStore;
        this.idxCnt = hashUniqIndexes.size() + hashIndexes.size() + sortedUniqIndexes.size() + sortedIndexes.size();

    }

    @Override
    public CompletableFuture<VersionChain<Tuple>> insert(Tuple row, UUID txId) {
        TxState txState = localState(txId);

        VersionChain<Tuple> rowId = rowStore.insert(row, txId);

        txState.addWrite(rowId);

        List<CompletableFuture> futs = new ArrayList<>(idxCnt);

        // TODO FIXME refactor indexing and locking logic depending on index type.
        for (Entry<Integer, HashIndex<VersionChain<Tuple>>> entry : hashUniqIndexes.entrySet()) {
            int col = entry.getKey();

            Tuple newVal = row.select(col);

            Lock lock = entry.getValue().lockTable().getOrAddEntry(newVal);

            txState.addLock(lock);

            futs.add(lock.acquire(txId, LockMode.X).thenAccept(ignored -> {
                Cursor<VersionChain<Tuple>> rowIds = entry.getValue().scan(newVal);

                VersionChain<Tuple> rowId0;

                while((rowId0 = rowIds.next()) != null) {
                    if (rowId0 == rowId) {
                        continue;
                    }

                    // TODO FIXME this is not protected by lock
                    if (rowStore.get(rowId0, txId, tuple -> tuple.select(col).equals(newVal)) != null)
                        throw new UniqueException("Failed to insert the row: duplicate index col=" + col + " key=" + newVal);
                }

                HashIndex<VersionChain<Tuple>> index = entry.getValue();

                if (index.insert(newVal, rowId)) {
                    // Undo insertion only if this transactions inserts a new entry.
                    txState.addUndo(() -> index.remove(newVal, rowId));
                }
            }));
        }

        return CompletableFuture.allOf(futs.toArray(new CompletableFuture[0])).thenApply(ignored -> rowId);
    }

    @Override
    public CompletableFuture<Tuple> update(VersionChain<Tuple> rowId, Tuple newRow, UUID txId) {
        Lock lock = rowStore.lockTable().getOrAddEntry(rowId);

        TxState txState = localState(txId);

        txState.addLock(lock);

        return lock.acquire(txId, LockMode.X).thenCompose(ignored -> {
            Tuple oldRow = rowStore.update(rowId, newRow, txId);

            txState.addWrite(rowId);

            List<CompletableFuture> futs = new ArrayList<>(idxCnt);

            for (Entry<Integer, HashIndex<VersionChain<Tuple>>> entry : hashUniqIndexes.entrySet()) {
                int col = entry.getKey();

                Tuple oldVal = oldRow == Tuple.TOMBSTONE ? Tuple.TOMBSTONE : oldRow.select(col);
                Tuple newVal = newRow == Tuple.TOMBSTONE ? Tuple.TOMBSTONE : newRow.select(col);

                if (!oldVal.equals(newVal)) {
                    if (oldVal.length() > 0) {
                        Lock lock0 = entry.getValue().lockTable().getOrAddEntry(oldVal);

                        txState.addLock(lock0);

                        futs.add(lock0.acquire(txId, LockMode.X));

                        // Do not remove bookmarks due to multi-versioning.
                    }

                    // TODO FIXME remove copypaste.
                    if (newVal.length() > 0) {
                        Lock lock0 = entry.getValue().lockTable().getOrAddEntry(newVal);

                        txState.addLock(lock0);

                        futs.add(lock0.acquire(txId, LockMode.X).thenAccept(ignored0 -> {
                            Cursor<VersionChain<Tuple>> rowIds = entry.getValue().scan(newVal);

                            VersionChain<Tuple> rowId0;

                            while((rowId0 = rowIds.next()) != null) {
                                if (rowId0 == rowId) {
                                    continue;
                                }

                                if (rowStore.get(rowId0, txId, tuple -> tuple.select(col).equals(newVal)) != null)
                                    throw new UniqueException("Failed to insert the row: duplicate index col=" + col + " key=" + newVal);
                            }

                            if (entry.getValue().insert(newVal, rowId)) {
                                txState.addUndo(() -> entry.getValue().remove(newVal, rowId));
                            } else {
                                throw new UniqueException("Failed to insert the row: duplicate index col=" + col + " key=" + newVal);
                            }
                        }));
                    }
                }
            }

            return CompletableFuture.allOf(futs.toArray(new CompletableFuture[0])).thenApply(ignored0 -> oldRow);
        });
    }

    @Override
    public CompletableFuture<Tuple> remove(VersionChain<Tuple> rowId, UUID txId) {
        Lock lock = rowStore.lockTable().getOrAddEntry(rowId);

        TxState txState = localState(txId);

        txState.addLock(lock);

        return lock.acquire(txId, LockMode.X).thenApply(ignored -> {
            Tuple removed = rowStore.update(rowId, Tuple.TOMBSTONE, txId);

            txState.addWrite(rowId);

            // Do not remove bookmarks due to multi-versioning.

            return removed;
        });
    }

    @Override
    public CompletableFuture<Tuple> get(VersionChain<Tuple> rowId, UUID txId) {
        Lock lock = rowStore.lockTable().getOrAddEntry(rowId);

        TxState txState = localState(txId);

        txState.addLock(lock);

        return lock.acquire(txId, LockMode.S).thenApply(x -> rowId.resolve(txId, null, null));
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
        }
        else if (query instanceof EqQuery) {
            EqQuery query0 = (EqQuery) query;

            HashIndex<VersionChain<Tuple>> idx = hashIndexes.get(query0.col);

            if (idx == null)
                idx = hashUniqIndexes.get(query0.col);

            if (idx == null)
                throw new IllegalArgumentException("Hash index not found for col=" + query0.col);

            AtomicReference<Cursor<VersionChain<Tuple>>> first = new AtomicReference<>();

            HashIndex<VersionChain<Tuple>> finalIdx = idx;

            return new AsyncCursor<VersionChain<Tuple>>() {
                @Override
                public CompletableFuture<VersionChain<Tuple>> nextAsync() {
                    Cursor<VersionChain<Tuple>> iter = first.get();
                    if (iter == null) {
                        Lock lock = finalIdx.lockTable().getOrAddEntry(query0.queryKey);

                        txState.addLock(lock);

                        return lock.acquire(txId, LockMode.S).thenApply(ignored -> {
                            Cursor<VersionChain<Tuple>> iter0 = finalIdx.scan(query0.queryKey);

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
