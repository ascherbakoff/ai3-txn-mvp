package com.ascherbakoff.ai3.tracker;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.replication.Command;
import com.ascherbakoff.ai3.replication.Put;
import com.ascherbakoff.ai3.replication.Request;
import com.ascherbakoff.ai3.replication.Response;
import com.ascherbakoff.ai3.table.KvTable;
import com.ascherbakoff.ai3.table.Tuple;
import java.lang.System.Logger.Level;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.jetbrains.annotations.Nullable;

public class Node {
    private static System.Logger LOGGER = System.getLogger(Node.class.getName());

    private final NodeId nodeId;

    public Node(NodeId nodeId) {
        this.nodeId = nodeId;
    }

    private State state = State.STOPPED;

    private Timestamp lwm = Timestamp.min();

    private Timestamp clock = Timestamp.min(); // Lamport clocks

    private Executor executor = Executors.newSingleThreadExecutor();

    private KvTable<Integer, String> table = new KvTable<>();

    private TreeMap<Timestamp, Command> traceMap = new TreeMap<>();

    private AtomicInteger idGen = new AtomicInteger();

    private TrackerState trackerState = new TrackerState(Timestamp.min(), null, Map.of());

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
            // Update logical clocks.
            if (request.getTs().compareTo(clock) > 0) {
                Node.this.clock = request.getTs();
            } else {
                Node.this.clock = Node.this.clock.adjust(1);
            }

            if (request.getLwm().compareTo(Node.this.lwm) > 0) {
                Node.this.lwm = request.getLwm(); // Ignore stale sync requests - this is safe.
            }

            if (request.getPayload() != null) {
                request.getPayload().accept(Node.this, resp);
                traceMap.put(request.getTs(), request.getPayload());
                assert traceMap.headMap(Node.this.lwm, true).size() == Node.this.lwm.getCounter();
            } else {
                resp.complete(new Response(Node.this.clock));
            }

        });

        return resp;
    }

    public void visit(Put put, CompletableFuture<Response> resp) {
        UUID id = new UUID(0, idGen.getAndIncrement());
        table.insert(Tuple.create(put.getKey(), put.getValue()), id).thenAccept(new Consumer<Integer>() {
            @Override
            public void accept(Integer key) {
                resp.complete(new Response(Node.this.clock)); // TODO send key.
            }
        });
    }

    public synchronized void refresh(Timestamp now, Node leaseholder, Map<NodeId, Tracker.State> nodeState) {
        if (now.compareTo(this.trackerState.last) < 0) // Ignore stale updates.
            return;

        this.trackerState = new TrackerState(now, leaseholder, nodeState);

        if (id().equals(leaseholder.nodeId)) {
            LOGGER.log(Level.INFO, "I'm the leasholder for {0}-{1}, now={2}", this.trackerState.last, this.trackerState.last.adjust(Tracker.LEASE_DURATION), clock);
        }
    }

    public synchronized void update(Timestamp clock) {
        if (clock.compareTo(this.clock) > 0)
            this.clock = clock;
    }

    public Timestamp getClock() {
        return clock;
    }

    enum State {
        STARTED, STOPPED
    }

    private static class TrackerState {
        final Timestamp last;

        final Node leaseholder;

        final Map<NodeId, Tracker.State> nodeState;

        TrackerState(Timestamp last, @Nullable Node leaseholder, Map<NodeId, Tracker.State> nodeState) {
            this.last = last;
            this.leaseholder = leaseholder;
            this.nodeState = nodeState;
        }
    }
}
