package com.ascherbakoff.ai3.lock;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

public class LockTable {
    public static final boolean[][] MATRIX = {
            {true, true, true, true, false},
            {true, true, false, false, false},
            {true, false, true, false, false},
            {true, false, false, false, false},
            {false, false, false, false, false},
    };

    protected final ConcurrentHashMap<Object, Lock> table;

    public LockTable(int size) {
        this.table = new ConcurrentHashMap<>(size);
    }

    public Lock getOrAddEntry(Object key) {
        return table.computeIfAbsent(key, k -> new Lock());
    }

    public void removeEntry(Object key, Lock lock) {
        synchronized (lock) {
            if (lock.waiters.isEmpty())
                table.remove(key);
        }
    }
}
