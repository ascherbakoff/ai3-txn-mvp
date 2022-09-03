package com.ascherbakoff.ai3.table;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.lock.LockTable;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import org.jetbrains.annotations.Nullable;

public class MVStoreImpl implements MVStore {
    private LockTable lockTable;

    public MVStoreImpl(LockTable lockTable) {
        this.lockTable = lockTable;
    }

    @Override
    public CompletableFuture<Void> put(Tuple row, UUID txId) {
        return null;
    }

    @Override
    public CompletableFuture<Tuple> remove(Tuple keyTuple, UUID txId) {
        return null;
    }

    @Override
    public Cursor<Tuple> scan(String idxName, @Nullable Predicate<Tuple> filter, UUID txId) {
        return null;
    }

    @Override
    public Cursor<Tuple> scan(String idxName, @Nullable Predicate<Tuple> filter, Timestamp readTs) {
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
