package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.cluster.Node;
import com.ascherbakoff.ai3.cluster.NodeId;
import com.ascherbakoff.ai3.cluster.Tracker.State;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class LeaseProposed implements Command {
    private final String name;
    private final Timestamp from;
    private final Map<NodeId, State> nodeState;

    public LeaseProposed(String name, Timestamp from, Map<NodeId, State> nodeState) {
        this.name = name;
        this.from = from;
        this.nodeState = nodeState;
    }

    @Override
    public void accept(Node node, Request request, CompletableFuture<Response> resp) {
        node.visit(this, request, resp);
    }

    /**
     * @return Name.
     */
    public String name() {
        return name;
    }

    public Timestamp from() {
        return from;
    }

    /**
     * @return Node state.
     */
    public Map<NodeId, State> nodeState() {
        return nodeState;
    }
}
