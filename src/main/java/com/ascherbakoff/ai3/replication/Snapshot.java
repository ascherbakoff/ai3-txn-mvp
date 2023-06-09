package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.cluster.Node;
import java.util.concurrent.CompletableFuture;

public class Snapshot implements Command {
    private final Timestamp low;
    private final Timestamp high;
    private final long cntr;

    public Snapshot(long cntr, Timestamp low, Timestamp high) {
        this.cntr = cntr;
        this.low = low;
        this.high = high;
    }

    public long getCntr() {
        return cntr;
    }

    public Timestamp getLow() {
        return low;
    }

    public Timestamp getHigh() {
        return high;
    }

    @Override
    public void accept(Node node, Request request, CompletableFuture<Response> resp) {
        node.visit(this, request, resp);
    }
}
