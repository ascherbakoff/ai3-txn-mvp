package com.ascherbakoff.ai3.table;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.lock.DeadlockPrevention;
import com.ascherbakoff.ai3.lock.Lock;
import com.ascherbakoff.ai3.lock.LockMode;
import com.ascherbakoff.ai3.lock.LockTable;
import com.ascherbakoff.ai3.lock.Locker;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

public class MVStoreImpl implements MVStore {
    private final VersionChainRowStore<Tuple> rowStore;
    private LockTable lockTable;
    private int[][] indexes;
    private HashIndex<VersionChain<Tuple>> primaryIndex = new HashIndexImpl<>(true);

    public MVStoreImpl(int[][] indexes) {
        this.lockTable = new LockTable(10, true, DeadlockPrevention.none());
        this.rowStore = new VersionChainRowStore<Tuple>();
    }

    @Override
    public CompletableFuture<Void> put(Tuple row, UUID txId) {
        Tuple pk = row.fields(indexes[0]);

        return lockTable.getOrAddEntry(pk).acquire(txId, LockMode.X).thenAccept(new Consumer<Void>() {
            @Override
            public void accept(Void ignored) {
                VersionChain<Tuple> rowId = rowStore.insert(row, txId);

                primaryIndex.insert(pk, rowId);
            }
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
    public CompletableFuture<Boolean> commit(UUID txId, Timestamp commitTs) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> abort(UUID txId) {
        return null;
    }
}
