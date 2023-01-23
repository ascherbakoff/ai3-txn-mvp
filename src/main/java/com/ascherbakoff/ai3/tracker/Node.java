package com.ascherbakoff.ai3.tracker;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.replication.Request;
import com.ascherbakoff.ai3.replication.Response;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

public class Node {
    private final NodeId nodeId;

    public Node(NodeId nodeId) {
        this.nodeId = nodeId;
    }

    private State state = State.STOPPED;

    private Timestamp lwm = Timestamp.min();

    private Executor executor = Executors.newSingleThreadExecutor();

    public void start() {
        this.state = State.STARTED;
    }

    public void stop() {
        this.state = State.STOPPED;
    }

    public NodeId id() {
        return nodeId;
    }

    /**
     * Returns the safe timestamp.
     *
     * @return The timestamp.
     */
    public Timestamp getLwm() {
        return lwm;
    }

    public CompletableFuture<Response> accept(Request request) {
        return CompletableFuture.supplyAsync(new Supplier<Response>() {
            @Override
            public Response get() {
                if (request.getLwm().compareTo(Node.this.lwm) > 0) {
                    Node.this.lwm = request.getLwm(); // Ignore stale sync requests - this is safe.
                }

                switch (request.getType()) {
                    case DATA:
                        // TODO handle request.
                        break;
                }

                return new Response();
            }
        }, executor);
    }

    enum State {
        STARTED, STOPPED
    }
}
