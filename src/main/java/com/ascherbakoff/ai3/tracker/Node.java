package com.ascherbakoff.ai3.tracker;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.replication.Command;
import com.ascherbakoff.ai3.replication.Put;
import com.ascherbakoff.ai3.replication.Request;
import com.ascherbakoff.ai3.replication.Response;
import com.ascherbakoff.ai3.table.KvTable;
import com.ascherbakoff.ai3.table.Tuple;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class Node {
    private final NodeId nodeId;

    public Node(NodeId nodeId) {
        this.nodeId = nodeId;
    }

    private State state = State.STOPPED;

    private Timestamp lwm = Timestamp.min();

    private Executor executor = Executors.newSingleThreadExecutor();

    private KvTable<Integer, String> table = new KvTable<>();

    private TreeMap<Timestamp, Command> traceMap = new TreeMap<>();

    private AtomicInteger idGen = new AtomicInteger();

    public void init() {

    }

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
        CompletableFuture<Response> resp = new CompletableFuture<>();

        executor.execute(() -> {
            if (request.getLwm().compareTo(Node.this.lwm) > 0) {
                Node.this.lwm = request.getLwm(); // Ignore stale sync requests - this is safe.
            }

            if (request.getPayload() != null) {
                request.getPayload().accept(Node.this, resp);
                traceMap.put(request.getTs(), request.getPayload());
                assert traceMap.headMap(Node.this.lwm, true).size() == Node.this.lwm.getCounter();
            } else {
                resp.complete(new Response());
            }

        });

        return resp;
    }

    public void visit(Put put, CompletableFuture<Response> resp) {
        UUID id = new UUID(0, idGen.getAndIncrement());
        table.insert(Tuple.create(put.getKey(), put.getValue()), id).thenAccept(new Consumer<Integer>() {
            @Override
            public void accept(Integer key) {
                resp.complete(new Response()); // TODO send key.
            }
        });
    }

    enum State {
        STARTED, STOPPED
    }
}
