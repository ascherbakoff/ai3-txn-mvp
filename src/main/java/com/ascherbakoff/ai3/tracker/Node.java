package com.ascherbakoff.ai3.tracker;

import com.ascherbakoff.ai3.clock.Clock;
import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.replication.Lease;
import com.ascherbakoff.ai3.replication.Put;
import com.ascherbakoff.ai3.replication.Request;
import com.ascherbakoff.ai3.replication.Response;
import com.ascherbakoff.ai3.table.KvTable;
import com.ascherbakoff.ai3.table.Tuple;
import java.lang.System.Logger.Level;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
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

    private Clock clock = new Clock();

    private Executor executor = Executors.newSingleThreadExecutor();

    private KvTable<Integer, String> table = new KvTable<>();

    private AtomicInteger idGen = new AtomicInteger();

    private TrackerState trackerState = new TrackerState(Timestamp.min(), null, Map.of());

    private Map<String, Group> groups = new HashMap<>();

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
            clock.onRequest(request.getTs());

            if (request.getLwm() != null && request.getLwm().compareTo(Node.this.lwm) > 0) {
                Node.this.lwm = request.getLwm(); // Ignore stale sync requests - this is safe.
            }

            if (request.getPayload() != null) {
                request.getPayload().accept(Node.this, resp); // Process request async.
            } else {
                resp.complete(new Response(Node.this.clock.now()));
            }
        });

        return resp;
    }

    public void visit(Put put, CompletableFuture<Response> resp) {
        UUID id = new UUID(0, idGen.getAndIncrement());
        table.insert(Tuple.create(put.getKey(), put.getValue()), id).thenAccept(new Consumer<Integer>() {
            @Override
            public void accept(Integer key) {
                resp.complete(new Response(Node.this.clock.now())); // TODO send key.
            }
        });
    }

    public void visit(Lease lease, CompletableFuture<Response> resp) {
        refresh(lease.name(), lease.from(), lease.candidate(), lease.nodeState()).thenAccept(ignored -> {
            resp.complete(new Response(Node.this.clock.now()));
        });
    }

    public CompletableFuture<Void> refresh(String grp, Timestamp now, NodeId leaseholder, Map<NodeId, Tracker.State> nodeState) {
        if (now.compareTo(this.trackerState.last) < 0) // Ignore stale updates.
            return CompletableFuture.completedFuture(null);

        Group group = this.groups.get(grp);
        if (group == null) {
            group = new Group(grp);
            this.groups.put(grp, group);
        }

        group.setLeaseHolder(leaseholder);
        group.setLease(now);
        for (Entry<NodeId, Tracker.State> entry : nodeState.entrySet()) {
            group.setState(entry.getKey(), entry.getValue());
        }

        if (id().equals(leaseholder)) {
            LOGGER.log(Level.INFO, "I am the leasholder: [interval={0}:{1}, now={2}, nodeId={3}]", this.trackerState.last, this.trackerState.last.adjust(Tracker.LEASE_DURATION), clock, nodeId);
        } else {
            LOGGER.log(Level.INFO, "Refresh leasholder: [interval={0}:{1}, now={2}], nodeId={3}", this.trackerState.last, this.trackerState.last.adjust(Tracker.LEASE_DURATION), clock, nodeId);
        }

        return CompletableFuture.completedFuture(null);
    }

    public Group group(String grp) {
        return groups.get(grp);
    }

    public Clock clock() {
        return clock;
    }

    enum State {
        STARTED, STOPPED
    }

    private static class TrackerState {
        final Timestamp last;

        final NodeId leaseholder;

        final Map<NodeId, Tracker.State> nodeState;

        TrackerState(Timestamp last, @Nullable NodeId leaseholder, Map<NodeId, Tracker.State> nodeState) {
            this.last = last;
            this.leaseholder = leaseholder;
            this.nodeState = nodeState;
        }
    }
}
