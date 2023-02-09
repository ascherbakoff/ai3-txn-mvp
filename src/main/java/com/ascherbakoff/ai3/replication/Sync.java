package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.cluster.Node;
import java.util.concurrent.CompletableFuture;

public class Sync implements Command {
    private Timestamp lwm;

    public Sync(Timestamp lwm) {
        this.lwm = lwm;
    }

    public Timestamp getLwm() {
        return lwm;
    }

    @Override
    public void accept(Node node, Request request, CompletableFuture<Response> resp) {
        node.visit(this, request, resp);
    }
}
