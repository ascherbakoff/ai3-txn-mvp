package com.ascherbakoff.ai3.cluster;

import java.util.concurrent.CompletableFuture;

public class Read {
    int key;
    CompletableFuture<Integer> fut;

    public Read(int key, CompletableFuture<Integer> fut) {
        this.key = key;
        this.fut = fut;
    }

    public int getKey() {
        return key;
    }

    public CompletableFuture<Integer> getFut() {
        return fut;
    }
}
