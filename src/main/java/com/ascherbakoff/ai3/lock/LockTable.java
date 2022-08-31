package com.ascherbakoff.ai3.lock;

import java.util.concurrent.ConcurrentHashMap;

public class LockTable {
    protected static final boolean[][] COMPAT_MATRIX = {
            {true, true, true, true, false},
            {true, true, false, false, false},
            {true, false, true, false, false},
            {true, false, false, false, false},
            {false, false, false, false, false},
    };

    protected static final LockMode[][] UPGRADE_MATRIX = {
            {LockMode.IS, LockMode.IX, LockMode.S, LockMode.SIX, LockMode.X},
            {LockMode.IX, LockMode.IX, LockMode.SIX, LockMode.SIX, LockMode.X},
            {LockMode.S, LockMode.SIX, LockMode.S, LockMode.SIX, LockMode.X},
            {LockMode.SIX, LockMode.SIX, LockMode.SIX, LockMode.SIX, LockMode.X},
            {LockMode.X, LockMode.X, LockMode.X, LockMode.X, LockMode.X},
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
            if (lock.waiters.isEmpty()) {
                table.remove(key);
            }
        }
    }
}
