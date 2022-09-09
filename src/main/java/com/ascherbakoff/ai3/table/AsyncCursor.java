package com.ascherbakoff.ai3.table;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import org.jetbrains.annotations.TestOnly;

public interface AsyncCursor<T> {
    CompletableFuture<T> nextAsync();

    @TestOnly
    default CompletableFuture<List<T>> loadAll(List<T> x) {
        return nextAsync().thenApply(t -> {
            if (t != null)
                x.add(t);
            return t;
        }).thenComposeAsync(t -> t == null ? CompletableFuture.completedFuture(x) : loadAll(x));
    }

    default CompletableFuture<Boolean> visit(Predicate<T> visitor) {
        return nextAsync().thenApply(t -> {
            if (t != null) {
                return visitor.test(t);
            }
            return null;
        }).thenComposeAsync(t -> t == null ? CompletableFuture.completedFuture(false) : t ? CompletableFuture.completedFuture(t) : visit(visitor));
    }
}
