package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.cluster.Node;
import java.util.concurrent.CompletableFuture;

public class Replicate implements Command {
    private Timestamp lwm;
    private Object data;

    public Replicate(Timestamp lwm, Object data) {
        this.lwm = lwm;
        this.data = data;
    }

    public Timestamp getLwm() {
        return lwm;
    }

    public Object getData() {
        return data;
    }

    @Override
    public void accept(Node node, Request request, CompletableFuture<Response> resp) {
        node.visit(this, request, resp);
    }
}
