package com.ascherbakoff.ai3.table;

import com.ascherbakoff.ai3.lock.DeadlockPrevention;
import com.ascherbakoff.ai3.lock.LockTable;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

public class KvTable<K extends Comparable<K>, V extends Comparable<V>> {
    MVStore store;

    public KvTable() {
        VersionChainRowStore<Tuple> rowStore = new VersionChainRowStore<>();
        store = new MVStoreImpl(
                rowStore,
                new LockTable(10, true, DeadlockPrevention.none()),
                Map.of(0, new HashUniqueIndex(0, new LockTable(10, true, DeadlockPrevention.none()), new HashIndexStoreImpl<>(), rowStore)) // PK
        );
    }

    public CompletableFuture<K> insert(Tuple t, UUID txId) {
        return store.insert(t, txId).thenApply(new Function<VersionChain<Tuple>, K>() {
            @Override
            public K apply(VersionChain<Tuple> chain) {
                Tuple inserted = chain.resolve(txId, null, null);

                Objects.requireNonNull(inserted);

                assert inserted.equals(t);

                return t.get(0);
            }
        });
    }

    public CompletableFuture<V> get(K key, UUID txId) {
        Tuple search = Tuple.create(key);
        AsyncCursor<VersionChain<Tuple>> query = store.query(new EqQuery(0, search), txId);

        return query.nextAsync().thenApply(chain -> {
            if (chain == null)
                return null;

            Tuple tuple = chain.resolve(txId, null, tup -> tup == Tuple.TOMBSTONE ? false : tup.select(0).equals(search));

            return tuple == null ? null : tuple.get(1);
        });
    }

    CompletableFuture<Tuple> update(Tuple newValue, UUID txId) {
        Tuple search = Tuple.create(newValue.get(0));
        AsyncCursor<VersionChain<Tuple>> query = store.query(new EqQuery(0, search), txId);

        return query.nextAsync().thenCompose(chain -> {
            if (chain == null)
                return CompletableFuture.completedFuture(null);

            return store.update(chain, newValue, txId);
        });
    }

    CompletableFuture<Tuple> remove(K key, UUID txId) {
        Tuple search = Tuple.create(key);
        AsyncCursor<VersionChain<Tuple>> query = store.query(new EqQuery(0, search), txId);

        return query.nextAsync().thenCompose(chain -> {
            if (chain == null)
                return CompletableFuture.completedFuture(null);

            return store.remove(chain, txId);
        });
    }
}
