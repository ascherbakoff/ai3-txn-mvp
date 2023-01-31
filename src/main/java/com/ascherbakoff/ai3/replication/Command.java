package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.cluster.Node;
import java.util.concurrent.CompletableFuture;

public interface Command {
    void accept(Node node, CompletableFuture<Response> resp);
}
