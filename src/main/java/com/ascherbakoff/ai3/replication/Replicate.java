package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.cluster.Node;
import java.util.concurrent.CompletableFuture;

public class Replicate implements Command {
    private long cntr;
    private Object data;

    public Replicate(long cntr, Object data) {
        this.cntr = cntr;
        this.data = data;
    }

    public long getCntr() {
        return cntr;
    }

    public void setCntr(long cntr) {
        this.cntr = cntr;
    }

    public Object getData() {
        return data;
    }

    @Override
    public void accept(Node node, Request request, CompletableFuture<Response> resp) {
        node.visit(this, request, resp);
    }
}
