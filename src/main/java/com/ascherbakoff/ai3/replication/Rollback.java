package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.cluster.Node;
import java.util.concurrent.CompletableFuture;

public class Rollback implements Command {
    private final Timestamp ts;

    public Rollback(Timestamp ts) {
        this.ts = ts;
    }

    @Override
    public void accept(Node node, Request request, CompletableFuture<Response> resp) {
        node.visit(this, request, resp);
    }

    public Timestamp getTs() {
        return ts;
    }
}
