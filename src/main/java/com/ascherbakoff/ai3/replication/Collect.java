package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.cluster.Node;
import java.util.concurrent.CompletableFuture;

public class Collect implements Command {
    @Override
    public void accept(Node node, Request request, CompletableFuture<Response> resp) {
        node.visit(this, request, resp);
    }
}
