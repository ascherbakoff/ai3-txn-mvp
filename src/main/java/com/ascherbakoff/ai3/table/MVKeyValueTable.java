package com.ascherbakoff.ai3.table;

import com.ascherbakoff.ai3.clock.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.Nullable;

/**
 * Multi-versioned key-value table.
 * @param <K>
 * @param <V>
 */
public class MVKeyValueTable<K extends Comparable<K>, V extends Comparable<V>> {
    VersionChainRowStore<Map.Entry<K,V>> rowStore;

    Map<K, VersionChain<Map.Entry<K,V>>> pk;

    AtomicLong idGen = new AtomicLong();

    TreeMap<Timestamp, K> pending = new TreeMap<>();

    public MVKeyValueTable() {
        rowStore = new VersionChainRowStore<>();
        pk = new HashMap<>();
    }

    public synchronized @Nullable V put(K k, V v, Timestamp ts) {
        // TODO can use lock, but put is single threaded.
        VersionChain<Map.Entry<K,V>> chain = pk.get(k);

        Map.Entry<K,V> prev = chain == null ? null : chain.head();

        UUID txId = new UUID(0, idGen.incrementAndGet());
        if (chain == null) {
            chain = rowStore.insert(new MyEntry<>(k, v), txId);
            pk.put(k, chain);
        } else {
            rowStore.update(chain, new MyEntry<>(k, v), txId);
        }

        pending.put(ts, k);

        return prev == null ? null : prev.getValue();
    }

    public synchronized void commit(Timestamp commitTs) {
        Iterator<Entry<Timestamp, K>> iterator = pending.entrySet().iterator();

        while (iterator.hasNext()) {
            Entry<Timestamp, K> next = iterator.next();

            if (next.getKey().compareTo(commitTs) > 0)
                break;

            iterator.remove();

            VersionChain<Map.Entry<K,V>> chain = pk.get(next.getValue());

            assert chain != null;

            rowStore.commitWrite(chain, commitTs, null);
        }
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

    public synchronized @Nullable V remove(K key, Timestamp commitTs) {
        VersionChain<Map.Entry<K,V>> head = pk.get(key);

        if (head != null) {
            Map.Entry<K,V> head1 = head.head();

            UUID txId = new UUID(0, idGen.incrementAndGet());
            rowStore.update(head, null, txId);
            head.commitWrite(commitTs, txId);

            return head1.getValue();
        }

        return null;
    }

    public Cursor<Map.Entry<K, V>> scan(Timestamp ts) {
        return rowStore.scan(ts);
    }

    public void finish(Set<Timestamp> tss, boolean finish) {
        for (Timestamp ts: tss) {
            K k = pending.get(ts);

            if (k == null)
                return; // TODO warn

            VersionChain<Map.Entry<K,V>> chain = pk.get(k);

            if (finish)
                chain.commitWrite(ts, null);
            else
                chain.abortWrite(null);
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
