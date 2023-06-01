package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;
import java.util.concurrent.CompletableFuture;

public class Inflight {
    private final Timestamp ts;
    private final Replicate replicate;
    private final CompletableFuture<Response> ioFut;

    public Inflight(Timestamp now, Replicate replicate, CompletableFuture<Response> ioFut) {
        this.ts = now;
        this.replicate = replicate;
        this.ioFut = ioFut;
    }

    public Timestamp ts() {
        return ts;
    }

    public Replicate getReplicate() {
        return replicate;
    }

    public CompletableFuture<Response> ioFuture() {
        return ioFut;
    }
}
