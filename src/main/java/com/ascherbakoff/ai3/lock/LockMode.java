package com.ascherbakoff.ai3.lock;

public enum LockMode {
    IS, IX, S, SIX, X;

    public boolean compatible(LockMode mode) {
        return LockTable.MATRIX[ordinal()][mode.ordinal()];
    }
}
