package com.ascherbakoff.ai3.table;

import com.ascherbakoff.ai3.clock.Timestamp;
import java.lang.System.Logger.Level;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * Multi-versioned key-value store.
 * @param <K>
 * @param <V>
 */
public class MVKeyValueStore<K extends Comparable<K>, V extends Comparable<V>> {
    private static System.Logger LOGGER = System.getLogger(MVKeyValueStore.class.getName());

    // TODO not needed if PK is mandatory
    VersionChainRowStore<Map.Entry<K,V>> rowStore;

    Map<K, VersionChain<Map.Entry<K,V>>> pk;

    Map<Timestamp, K> pending = new HashMap<>();

    public MVKeyValueStore() {
        rowStore = new VersionChainRowStore<>();
        pk = new HashMap<>();
    }

    public synchronized @Nullable V put(K k, V v, Timestamp ts) {
        // TODO can use lock, but put is single threaded.
        VersionChain<Map.Entry<K,V>> chain = pk.get(k);

        Map.Entry<K,V> prev = chain == null ? null : chain.head();

        UUID txId = ts.toUUID();
        if (chain == null) {
            chain = rowStore.insert(new MyEntry<>(k, v), txId);
            pk.put(k, chain);
        } else {
            rowStore.update(chain, new MyEntry<>(k, v), txId);
        }

        pending.put(ts, k);

        return prev == null ? null : prev.getValue();
    }

    public synchronized @Nullable V get(K key, Timestamp ts) {
        VersionChain<Map.Entry<K,V>> chain = pk.get(key);

        if (chain == null) {
            return null;
        } else {
            Entry<K, V> resolved = chain.resolve(null, ts, null);
            return resolved == null ? null : resolved.getValue();
        }
    }

    public synchronized @Nullable V remove(K key, Timestamp ts) {
        VersionChain<Map.Entry<K,V>> head = pk.get(key);

        if (head != null) {
            Map.Entry<K,V> head1 = head.head();

            rowStore.update(head, null, ts.toUUID());
            pending.put(ts, key);

            return head1.getValue();
        }

        return null;
    }

    public Cursor<Map.Entry<K, V>> scan(Timestamp ts) {
        return rowStore.scan(ts);
    }

    public void finish(Set<Timestamp> tss, boolean finish) {
        for (Timestamp ts: tss) {
            K k = pending.remove(ts);

            if (k == null) {
                // It's possible to receive abort request for not started(non-existing) transaction.
                if (!finish)
                    LOGGER.log(Level.WARNING, "Failed to abort non existing transaction at ts={0}", ts);

                continue;
            }

            VersionChain<Map.Entry<K,V>> chain = pk.get(k);

            UUID txId = ts.toUUID();

            if (finish)
                chain.commitWrite(ts, txId);
            else {
                if (chain.next == null) {
                    pk.remove(k);

                    return;
                }

                chain.abortWrite(txId);
            }
        }
    }

    public synchronized VersionChainRowStore<Entry<K, V>> snapshot(@Nullable Timestamp low, Timestamp high) {
        Set<VersionChain<Entry<K, V>>> values = rowStore.getHeads();

        VersionChainRowStore<Entry<K, V>> store = new VersionChainRowStore<>();

        for (VersionChain<Entry<K, V>> value : values) {
            VersionChain<Entry<K, V>> cloned = value.clone(high);
            if (cloned != null)
                store.getHeads().add(cloned);
        }

        return store;
    }

    /**
     * Merges the snapshot with the current state.
     * TODO handle concurrent load.
     *
     * @param snapshot The snapshot.
     */
    public void setSnapshot(VersionChainRowStore<Entry<K, V>> snapshot) {
        for (VersionChain<Entry<K, V>> chain : snapshot.getHeads()) {
            pk.compute(chain.value.getKey(), (k, v) -> {
                if (v == null) {
                    rowStore.getHeads().add(chain); // TODO FIXME thread unsafe.
                    return chain;
                } else {
                    v.merge(chain);
                    return v;
                }
            });
        }
    }

    private static class MyEntry<K, V> implements Map.Entry<K,V> {
        K k;
        V v;

        MyEntry(K k, V v) {
            this.k = k;
            this.v = v;
        }

        @Override
        public K getKey() {
            return k;
        }

        @Override
        public V getValue() {
            return v;
        }

        @Override
        public V setValue(V value) {
            V prev = v;
            v = value;
            return prev;
        }
    }
}
