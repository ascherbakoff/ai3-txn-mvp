package com.ascherbakoff.ai3.cluster;

import com.ascherbakoff.ai3.clock.Clock;
import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.replication.Lease;
import com.ascherbakoff.ai3.replication.Put;
import com.ascherbakoff.ai3.replication.Replicator;
import com.ascherbakoff.ai3.replication.Replicator.Inflight;
import com.ascherbakoff.ai3.replication.Request;
import com.ascherbakoff.ai3.replication.Response;
import com.ascherbakoff.ai3.table.Tuple;
import java.lang.System.Logger.Level;
import java.util.HashMap;
import java.util.HashSet;
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

    private final Clock clock;

    private Executor executor = Executors.newSingleThreadExecutor();

    private Map<String, Group> groups = new HashMap<>();

    private final Topology top;

    public Node(NodeId nodeId, Topology top, Clock clock) {
        this.nodeId = nodeId;
        this.top = top;
        this.clock = clock;
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

    public CompletableFuture<Response> accept(Request request) {
        CompletableFuture<Response> resp = new CompletableFuture<>();

        executor.execute(() -> {
            // Update logical clocks.
            clock.onRequest(request.getTs());

            Group grp = groups.get(request.getGrp());

            if (request.getLwm() != null && request.getLwm().compareTo(grp.lwm) > 0) {
                grp.lwm = request.getLwm(); // Ignore stale sync requests - this is safe.
            }

            if (request.getPayload() != null) {
                request.getPayload().accept(Node.this, request, resp); // Process request async.
            } else {
                resp.complete(new Response(Node.this.clock.now()));
            }

            resp.complete(new Response(Node.this.clock.now()));
        });

        return resp;
    }

    public void visit(Put put, Request request, CompletableFuture<Response> resp) {
        Group grp = groups.get(request.getGrp());
        UUID txId = put.getTxId();
        Timestamp commitTs = clock.now();
        grp.table.put(put.getKey(), put.getValue(), commitTs);
        resp.complete(new Response(commitTs));
    }

    public void visit(Lease lease, Request request, CompletableFuture<Response> resp) {
        Timestamp now = Node.this.clock.now();
        refresh(now, lease.name(), lease.from(), lease.candidate(), lease.nodeState());
        resp.complete(new Response(now));
    }

    public boolean refresh(Timestamp now, String grp, Timestamp leaseStart, NodeId leaseholder, Map<NodeId, Tracker.State> nodeState) {
        Group group = this.groups.get(grp);
        if (group == null) {
            group = new Group(grp);
            this.groups.put(grp, group);
        }

        Timestamp prev = group.getLease();

        if (prev != null && leaseStart.compareTo(prev) < 0) // Ignore stale updates.
            return false;

        if (prev != null && leaseStart.compareTo(prev.adjust(Tracker.LEASE_DURATION)) < 0 && !leaseholder.equals(group.getLeaseHolder())) // Ignore stale updates, except refresh for current leaseholder. TODO test
            return false;

        group.setLeaseHolder(leaseholder);
        group.setLease(leaseStart);
        for (Entry<NodeId, Tracker.State> entry : nodeState.entrySet()) {
            group.setState(entry.getKey(), entry.getValue());
        }

        clock.onRequest(leaseStart); // Sync clocks to lease.

        if (id().equals(leaseholder)) {
            LOGGER.log(Level.INFO, "I am the leasholder: [interval={0}:{1}, at={2}, nodeId={3}]", leaseStart, leaseStart.adjust(Tracker.LEASE_DURATION), now, nodeId);
        } else {
            LOGGER.log(Level.INFO, "Refresh leasholder: [interval={0}:{1}, at={2}], nodeId={3}", leaseStart, leaseStart.adjust(Tracker.LEASE_DURATION), now, nodeId);
        }

        assert group.validLease(now, leaseholder);

        return true;
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

    public Result replicate(String grp, Put put) {
        Group group = groups.get(grp);

        if (!group.validLease(clock.now(), nodeId)) {
            Result res = new Result();
            res.setFuture(CompletableFuture.failedFuture(new IllegalStateException("Illegal lease")));
            return res;
        }

        Set<NodeId> nodeIds = group.getNodeState().keySet();

        CompletableFuture<Void> resFut = new CompletableFuture<>();

        Result res = new Result();
        res.setFuture(resFut);

        AtomicInteger majority = new AtomicInteger(0);

        UUID idd = UUID.randomUUID();

        for (NodeId id : nodeIds) {
            Replicator replicator = group.replicators.get(id);
            if (replicator == null) {
                replicator = new Replicator(this, id, grp, top); // TODO do we need to pass top ?
                group.replicators.put(id, replicator);
            }

            Request request = new Request(); // Creating the request copy is essential.
            request.setId(idd);
            request.setGrp(grp);
            request.setPayload(put);
            request.setLwm(replicator.getLwm());

            Inflight inflight = replicator.send(request);

            res.getPending().put(id, inflight);

            inflight.future().thenAccept(resp -> {
                int val = majority.incrementAndGet();
                if (val == nodeIds.size() / 2 + 1) {
                    LOGGER.log(Level.DEBUG, "All ack " + inflight.ts() + " req=" + idd+ " node=" + id);
                    resFut.complete(null);
                } else {
                    LOGGER.log(Level.DEBUG, "Ack " + inflight.ts()+ " req=" + idd + " node=" + id);
                }
            });
        }

        return res;
    }

    public void createReplicator(String grp, NodeId id) {
        Group group = groups.get(grp);

        Replicator replicator = group.replicators.get(id);
        if (replicator == null) {
            replicator = new Replicator(this, id, grp, top); // TODO do we need to pass top ?
            group.replicators.put(id, replicator);
        }
    }

    public static class Result {
        CompletableFuture<Void> res = new CompletableFuture<>();

        Map<NodeId, Inflight> pending = new HashMap<>();

        public CompletableFuture<Void> future() {
            return res;
        }

        public void setFuture(CompletableFuture<Void> res) {
            this.res = res;
        }

        public Map<NodeId, Inflight> getPending() {
            return pending;
        }

        public void setPending(Map<NodeId, Inflight> pending) {
            this.pending = pending;
        }
    }

    public @Nullable Integer get(String grp, int key, Timestamp ts) {
        Group group = groups.get(grp);
        assert group != null;

        return group.table.get(key, ts);
    }

    public CompletableFuture<Void> sync(String grp) {
        Group group = groups.get(grp);

        Timestamp now = clock.now();
        if (!group.validLease(now, nodeId)) {
            return CompletableFuture.failedFuture(new IllegalStateException("Illegal lease"));
        }

        Set<NodeId> nodeIds = new HashSet<>(group.getNodeState().keySet());
        nodeIds.remove(nodeId);

        CompletableFuture<Void> res = new CompletableFuture<>();

        AtomicInteger majority = new AtomicInteger(0);

        for (NodeId id : nodeIds) {
            Request r = new Request();
            r.setGrp(grp);
            r.setTs(now); // Propagate ts in idle sync.

            Replicator replicator = group.replicators.get(id);
            if (replicator == null) {
                replicator = new Replicator(this, id, grp, top);
                group.replicators.put(id, replicator);
            }

            r.setLwm(replicator.getLwm());

            replicator.idleSync(r).thenAccept(resp -> {
                int val = majority.incrementAndGet();
                if (val == nodeIds.size())
                    res.complete(null);
            });
        }

        return res;
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
