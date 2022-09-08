package com.ascherbakoff.ai3.table;

public class EqQuery implements Query {
    int col;
    Tuple queryKey;

    public EqQuery(int col, Tuple queryKey) {
        this.col = col;
        this.queryKey = queryKey;
    }
}
