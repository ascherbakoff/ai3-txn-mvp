package com.ascherbakoff.ai3.table;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.lock.LockTable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import org.jetbrains.annotations.Nullable;

/**
 * TODO remove a specific version.
 * @param <T>
 */
class VersionChainRowStore<T> implements RowStore<VersionChain<T>, T>, Lockable {
    private Set<VersionChain<T>> heads = new HashSet<>();
    private LockTable lockTable;

    VersionChainRowStore(LockTable lockTable) {
        this.lockTable = lockTable;
    }

    @Nullable
    @Override
    public T get(VersionChain<T> rowId, UUID txId, @Nullable Predicate<T> filter) {
        return rowId.resolve(txId, null, filter);
    }

    @Nullable
    @Override
    public T get(VersionChain<T> rowId, Timestamp timestamp, @Nullable Predicate<T> filter) {
        return rowId.resolve(null, timestamp, filter);
    }

    @Override
    public VersionChain<T> insert(T newRow, UUID txId) {
        VersionChain<T> head = new VersionChain<>(txId, null, null, newRow, null);
        heads.add(head);
        return head;
    }

    @Override
    public void update(VersionChain<T> rowId, @Nullable T newRow, UUID txId) {
        rowId.addWrite(newRow, txId);
    }

    @Override
    public boolean remove(VersionChain<T> rowId) {
        return heads.remove(rowId);
    }

    @Override
    public void commitWrite(VersionChain<T> rowId, Timestamp timestamp, UUID txId) {
        rowId.commitWrite(timestamp, txId);
    }

    @Override
    public void abortWrite(VersionChain<T> rowId, UUID txId) {
        if (rowId.next == null) {
            remove(rowId);

            return;
        }

        rowId.abortWrite(txId);
    }

    @Override
    public Cursor<T> scan(UUID txId) {
        Iterator<VersionChain<T>> iterator = heads.iterator();

        return new Cursor<T>() {
            @Nullable
            @Override
            public T next() {
                while(true) {
                    if (!iterator.hasNext())
                        return null;

                    VersionChain<T> next = iterator.next();

                    T val = next.resolve(txId, null, null);

                    if (val != null)
                        return val;
                }
            }
        };
    }

    @Override
    public Cursor<T> scan(Timestamp timestamp) {
        Iterator<VersionChain<T>> iterator = heads.iterator();

        return new Cursor<T>() {
            @Nullable
            @Override
            public T next() {
                while(true) {
                    if (!iterator.hasNext())
                        return null;

                    VersionChain<T> next = iterator.next();

                    T val = next.resolve(null, timestamp, null);

                    if (val != null)
                        return val;
                }
            }
        };
    }

    @Override
    public LockTable lockTable() {
        return lockTable;
    }
}
