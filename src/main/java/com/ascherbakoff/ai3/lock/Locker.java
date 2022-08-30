package com.ascherbakoff.ai3.lock;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class Locker extends CompletableFuture<Void> {
    final UUID lockerId;

    LockMode mode;

    /**
     * @param lockerId Locker id.
     * @param mode Requested mode.
     */
    public Locker(UUID lockerId, LockMode mode) {
        this.lockerId = lockerId;
        this.mode = mode;
    }
}
