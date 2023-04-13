package com.ascherbakoff.ai3.table;

import com.ascherbakoff.ai3.clock.Timestamp;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import org.jetbrains.annotations.Nullable;

/**
 * TODO remove a specific version.
 * @param <T>
 */
public class VersionChainRowStore<T> implements RowStore<VersionChain<T>, T> {
    private Set<VersionChain<T>> heads;

    public VersionChainRowStore() {
        heads = Collections.newSetFromMap(new ConcurrentHashMap<>());
    }

    public VersionChainRowStore(Set<VersionChain<T>> heads) {
        this.heads = heads;
    }

    @Override
    public T get(VersionChain<T> rowId, UUID txId, @Nullable Predicate<T> filter) {
        return rowId.resolve(txId, null, filter);
    }

    @Override
    public T get(VersionChain<T> rowId, Timestamp timestamp, @Nullable Predicate<T> filter) {
        return rowId.resolve(null, timestamp, filter);
    }

    @Override
    public VersionChain<T> insert(T newRow, UUID txId) {
        VersionChain<T> head = new VersionChain<>(txId, null, null, newRow);
        heads.add(head);
        return head;
    }

    @Override
    public T update(VersionChain<T> rowId, T newRow, UUID txId) {
        return rowId.addWrite(newRow, txId);
    }


    @Override
    public void commitWrite(VersionChain<T> rowId, Timestamp timestamp, UUID txId) {
        rowId.commitWrite(timestamp, txId);
    }

    @Override
    public void abortWrite(VersionChain<T> rowId, UUID txId) {
        if (rowId.next == null) {
            heads.remove(rowId); // TBD FIXME concurrent scan test

            return;
        }

        rowId.abortWrite(txId);
    }

    public Set<VersionChain<T>> getHeads() {
        return heads;
    }

    @Override
    public Cursor<VersionChain<T>> scan(UUID txId) {
        Iterator<VersionChain<T>> iterator = heads.iterator();

        return new Cursor<VersionChain<T>>() {
            @Nullable
            @Override
            public VersionChain<T> next() {
                while(true) {
                    if (!iterator.hasNext())
                        return null;

                    return iterator.next();
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
                while (true) {
                    if (!iterator.hasNext()) {
                        return null;
                    }

                    VersionChain<T> next = iterator.next();

                    T val = next.resolve(null, timestamp, null);

                    if (val != null) {
                        return val;
                    }
                }
            }
        };
    }
}
