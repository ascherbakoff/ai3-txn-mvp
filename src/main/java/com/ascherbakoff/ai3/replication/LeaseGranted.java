package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.cluster.Node;
import com.ascherbakoff.ai3.cluster.NodeId;
import com.ascherbakoff.ai3.cluster.Tracker.State;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class LeaseGranted implements Command {
    private final String name;
    private final Timestamp lwm;
    private final Timestamp from;
    private final NodeId candidate;
    private final Map<NodeId, State> nodeState;

    public LeaseGranted(String name, Timestamp from, NodeId candidate, Map<NodeId, State> nodeState, Timestamp lwm) {
        this.name = name;
        this.from = from;
        this.candidate = candidate;
        this.nodeState = nodeState;
        this.lwm = lwm;
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

    /**
     * @return Max LWM in a group.
     */
    public Timestamp lwm() {
        return lwm;
    }

    public Timestamp from() {
        return from;
    }

    /**
     * @return Candidate.
     */
    public NodeId candidate() {
        return candidate;
    }

    /**
     * @return Node state.
     */
    public Map<NodeId, State> nodeState() {
        return nodeState;
    }
}