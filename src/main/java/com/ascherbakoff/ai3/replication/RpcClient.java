package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.tracker.Node;
import com.ascherbakoff.ai3.tracker.NodeId;
import com.ascherbakoff.ai3.tracker.Topology;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class RpcClient {
    private Topology topology;

    public RpcClient(Topology topology) {
        this.topology = topology;
    }

    CompletableFuture<Response> send(NodeId nodeId, Request request) {
        Node node = topology.getNodeMap().get(nodeId);

        Objects.requireNonNull(node);

        return node.accept(request);
    }
}
