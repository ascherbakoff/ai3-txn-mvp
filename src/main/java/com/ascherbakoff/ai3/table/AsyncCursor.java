package com.ascherbakoff.ai3.table;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface AsyncCursor<T> {
    CompletableFuture<T> nextAsync();

    default CompletableFuture<List<T>> loadAll(List<T> x) {
        return nextAsync().thenApply(t -> {
            x.add(t);
            return x;
        }).thenCompose(this::loadAll);
    }
}
