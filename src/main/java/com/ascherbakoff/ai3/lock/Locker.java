package com.ascherbakoff.ai3.lock;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class Locker extends CompletableFuture<LockMode> {
    final UUID id;

    LockMode mode;

    public LockMode getMode() {
        return mode;
    }

    /**
     * @param id Locker id.
     * @param mode Requested mode.
     */
    public Locker(UUID id, LockMode mode) {
        this.id = id;
        this.mode = mode;
    }

    @Override
    public String toString() {
        return "Locker{" +
                "id=" + id +
                ", mode=" + mode +
                ", done=" + isDone() +
                '}';
    }
}
