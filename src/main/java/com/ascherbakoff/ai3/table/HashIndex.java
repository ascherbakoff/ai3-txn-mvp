package com.ascherbakoff.ai3.table;

interface HashIndex<T> extends Lockable {
    Cursor<T> scan(Tuple key);

    boolean insert(Tuple key, T rowId);

    boolean remove(Tuple key, T rowId);
}
