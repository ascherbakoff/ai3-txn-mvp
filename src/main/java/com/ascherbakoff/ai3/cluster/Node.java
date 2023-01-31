package com.ascherbakoff.ai3.cluster;

import com.ascherbakoff.ai3.clock.Clock;
import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.replication.Lease;
import com.ascherbakoff.ai3.replication.Put;
import com.ascherbakoff.ai3.replication.Replicator;
import com.ascherbakoff.ai3.replication.Replicator.Inflight;
import com.ascherbakoff.ai3.replication.Request;
import com.ascherbakoff.ai3.replication.Response;
import com.ascherbakoff.ai3.table.KvTable;
import com.ascherbakoff.ai3.table.Tuple;
import java.lang.System.Logger.Level;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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

    private State state = State.STOPPED;

    private Timestamp lwm = Timestamp.min();

    private Clock clock = new Clock();

    private Executor executor = Executors.newSingleThreadExecutor();

    private KvTable<Integer, String> table = new KvTable<>();

    private AtomicInteger idGen = new AtomicInteger();

    private TrackerState trackerState = new TrackerState(Timestamp.min(), null, Map.of());

    private Map<String, Group> groups = new HashMap<>();

    private final Topology top;


    public Node(NodeId nodeId, Topology top) {
        this.nodeId = nodeId;
        this.top = top;
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

        Timestamp at = clock.now();

        if (id().equals(leaseholder)) {
            LOGGER.log(Level.INFO, "I am the leasholder: [interval={0}:{1}, at={2}, nodeId={3}]", this.trackerState.last, this.trackerState.last.adjust(Tracker.LEASE_DURATION), at, nodeId);
        } else {
            LOGGER.log(Level.INFO, "Refresh leasholder: [interval={0}:{1}, at={2}], nodeId={3}", this.trackerState.last, this.trackerState.last.adjust(Tracker.LEASE_DURATION), at, nodeId);
        }

        assert group.validLease(at);

        return CompletableFuture.completedFuture(null);
    }

    public Group group(String grp) {
        return groups.get(grp);
    }

    public Clock clock() {
        return clock;
    }

    public @Nullable NodeId getLeaseHolder(String grpName) {
        Group group = groups.get(grpName);
        if (group == null)
            return null;

        Timestamp now = clock.now();

        Timestamp lease = group.getLease();

        if (lease != null && now.compareTo(lease.adjust(Tracker.LEASE_DURATION)) < 0)
            return group.getLeaseHolder();

        return null;
    }

    public CompletableFuture<Void> replicate(String grp, Put put) {
        Group group = groups.get(grp);
        if (!group.validLease(clock.now()))
            return CompletableFuture.failedFuture(new IllegalStateException("Not a leaseholder"));

        Set<NodeId> nodeIds = group.getNodeState().keySet();

        CompletableFuture<Void> res = new CompletableFuture<>();

        AtomicInteger majority = new AtomicInteger(0);

        for (NodeId id : nodeIds) {
            Replicator replicator = group.replicators.get(id);
            if (replicator == null) {
                replicator = new Replicator(this, id, top); // TODO do we need to pass top ?
                group.replicators.put(id, replicator);
            }

            Inflight inflight = replicator.send(put);

            inflight.future().thenAccept(resp -> {
                int val = majority.incrementAndGet();
                if (val == nodeIds.size() / 2 + 1)
                    res.complete(null);
            });
        }

        return res;
    }

    public int get(int key) {
        return 0;
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
