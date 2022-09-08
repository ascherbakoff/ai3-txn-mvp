package com.ascherbakoff.ai3.table;

import com.ascherbakoff.ai3.lock.LockTable;

public interface Lockable {
    LockTable lockTable();
}
