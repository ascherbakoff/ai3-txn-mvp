package com.ascherbakoff.ai3.table;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.cluster.Node;
import java.lang.System.Logger.Level;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.Nullable;

/**
 * Multi-versioned key-value store.
 * @param <K>
 * @param <V>
 */
public class MVKeyValueStore<K extends Comparable<K>, V extends Comparable<V>> {
    private static System.Logger LOGGER = System.getLogger(MVKeyValueStore.class.getName());

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
                    LOGGER.log(Level.WARNING, "Failed to abort non existing transaction with k={0} ts={1}", k, ts);

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
