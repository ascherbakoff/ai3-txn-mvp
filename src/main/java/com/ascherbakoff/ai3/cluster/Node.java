package com.ascherbakoff.ai3.cluster;

import com.ascherbakoff.ai3.clock.Clock;
import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.replication.Collect;
import com.ascherbakoff.ai3.replication.CollectResponse;
import com.ascherbakoff.ai3.replication.Configure;
import com.ascherbakoff.ai3.replication.Finish;
import com.ascherbakoff.ai3.replication.LeaseGranted;
import com.ascherbakoff.ai3.replication.LeaseProposed;
import com.ascherbakoff.ai3.replication.Put;
import com.ascherbakoff.ai3.replication.Replicate;
import com.ascherbakoff.ai3.replication.Replicator;
import com.ascherbakoff.ai3.replication.Replicator.Inflight;
import com.ascherbakoff.ai3.replication.Request;
import com.ascherbakoff.ai3.replication.Response;
import com.ascherbakoff.ai3.replication.RpcClient;
import com.ascherbakoff.ai3.replication.Sync;
import java.lang.System.Logger.Level;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.Nullable;

// TODO needs refactoring.
public class Node {
    private static System.Logger LOGGER = System.getLogger(Node.class.getName());

//    static {
//        Logger root = Logger.getLogger("");
//        root.setLevel(java.util.logging.Level.ALL);
//        for (Handler handler : root.getHandlers()) {
//            handler.setLevel(java.util.logging.Level.ALL);
//        }
//    }

    private final NodeId nodeId;

    private State state = State.STOPPED;

    private final Clock clock;

    private Map<String, Group> groups = new HashMap<>();

    private final Topology top;

    private RpcClient client;

    public Node(NodeId nodeId, Topology top, Clock clock) {
        this.nodeId = nodeId;
        this.top = top;
        this.clock = clock;
        this.client = new RpcClient(top);
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

        // Create unitialized group. TODO Any request targeting such group must be ignored.
        if (groups.get(request.getGrp()) == null) {
            this.groups.putIfAbsent(request.getGrp(), new Group(request.getGrp())); // TODO leaking threads
        }

        ExecutorService svc = groups.get(request.getGrp()).executorService;
        svc.execute(() -> {
            // Update logical clocks.
            clock.onRequest(request.getTs());

            request.getPayload().accept(Node.this, request, resp); // Process request async.
        });

        return resp;
    }

    public void visit(Replicate replicate, Request request, CompletableFuture<Response> resp) {
        Group grp = groups.get(request.getGrp());
        assert grp != null; // Created on before request processing.

        Timestamp now = clock.now();

        // Validates if a request fits the lease window.
        if (!grp.validLease(now, request.getSender())) {
            resp.complete(new Response(now, 1, "Illegal lease"));
            return;
        }

        if (!grp.validLease(request.getTs(), request.getSender())) {
            resp.complete(new Response(now, 1, "Illegal lease"));
            return;
        }

        grp.setLwm(replicate.getLwm());

        Object data = replicate.getData();
        if (data instanceof Put) {
            Put put = (Put) data;
            grp.store.put(put.getKey(), put.getValue(), request.getTs());
            resp.complete(new Response(now));
        } else if (data instanceof Configure) {
            Configure configure = (Configure) data;
            grp.setState(configure.getMembers(), request.getTs());
            resp.complete(new Response(now));
        } else {
            resp.complete(new Response(now, 1, "Unsupported command"));
        }
    }

    public void visit(Finish finish, Request request, CompletableFuture<Response> resp) {
        Group grp = groups.get(request.getGrp());

        grp.setLwm(finish.getLwm());

        if (finish.data()) {
            grp.store.finish(finish.getTs(), finish.finish());
        } else {
            grp.commitState(finish.getTs().iterator().next(), finish.finish());
        }
    }

    public void visit(LeaseGranted lease, Request request, CompletableFuture<Response> resp) {
        grant(lease.name(), lease.from(), lease.candidate(), lease.members(), lease.maxLwm(), resp);
    }

    public void visit(LeaseProposed lease, Request request, CompletableFuture<Response> resp) {
        propose(lease.name(), lease.from(), lease.members(), resp);
    }

    public void visit(Sync sync, Request request, CompletableFuture<Response> resp) {
        Group grp = groups.get(request.getGrp());

        grp.setLwm(sync.getLwm());

        resp.complete(new Response(clock.now()));
    }

    public void visit(Collect collect, Request request, CompletableFuture<Response> resp) {
        Group grp = groups.get(request.getGrp());

        resp.complete(new CollectResponse(grp.lwm, clock.now()));
    }

    private void propose(String grp, Timestamp leaseStart, Set<NodeId> members, CompletableFuture<Response> resp) {
        LOGGER.log(Level.INFO, "[grp={0}, from={1}, nodeId={2}]", grp, leaseStart, nodeId);

        NodeId candidate = nodeId; // Try assign myself.
        Group group = this.groups.get(grp);
        assert group != null;

        Timestamp prev = group.getLease();

        Timestamp now = clock.now();

        if (prev != null && leaseStart.compareTo(prev) < 0) { // Ignore stale updates.
            resp.complete(new Response(now, 1, "Lease request ignored (outdated)")); // TODO error code
            return;
        }

        boolean leaseExtended = false;

        if (prev != null && leaseStart.compareTo(prev.adjust(Tracker.LEASE_DURATION)) < 0) { // Ignore stale updates, except refresh for current leaseholder. TODO test
            if (!candidate.equals(group.getLeaseHolder())) {
                resp.complete(new Response(now, 1, "Lease request ignored (wrong candidate)")); // TODO error code
                return;
            }

            leaseExtended = true;
        }

        // Skip validation if lease is extended.
        if (leaseExtended) {
            resp.complete(new Response(now));

            // Asynchronously notify alive members.
            for (NodeId nodeId0 : members) {
                Request request = new Request();
                request.setGrp(group.getName());
                request.setTs(now);

                // Send current lwm to new nodes.
                Timestamp tmp = (!group.getMembers().contains(nodeId0)) ? group.lwm : null;
                request.setPayload(new LeaseGranted(group.getName(), leaseStart, group.getLeaseHolder(), members, tmp));

                if (top.getNode(nodeId0) == null)
                    continue;

                client.send(nodeId0, request);
            }

            return;
        }

        Map<NodeId, Timestamp> lwms = Collections.synchronizedMap(new HashMap<>());

        for (NodeId nodeId : members) {
            Request request = new Request();
            request.setGrp(grp);
            request.setTs(now);
            request.setPayload(new Collect());

            if (top.getNodeMap().get(nodeId) == null) {
                callback(now, lwms, nodeId, null, members, leaseStart, group, candidate, resp);
            } else {
                client.send(nodeId, request).orTimeout(Replicator.TIMEOUT_SEC, TimeUnit.SECONDS).thenAccept(response -> {
                    clock.onResponse(response.getTs());
                    CollectResponse cr = (CollectResponse) response;

                    // TODO report cause.
                    callback(now, lwms, nodeId, cr.getLwm(), members, leaseStart, group, candidate, resp);
                }).exceptionally(err -> {
                    callback(now, lwms, nodeId, null, members, leaseStart, group, candidate, resp);
                    return null; // TODO report cause.
                });
            }
        }
    }

    private void grant(String grp, Timestamp from, NodeId leaseholder, Set<NodeId> members, @Nullable Timestamp maxLwm, CompletableFuture<Response> resp) {
        Timestamp now = clock.now();

        Group group = this.groups.get(grp);
        assert group != null;

        Timestamp prev = group.getLease();

        if (prev != null && from.compareTo(prev) < 0) {// Ignore stale updates.
            resp.complete(new Response(now, 1, "Lease request ignored (outdated)")); // TODO error code
            return;
        }

        if (prev != null && from.compareTo(prev.adjust(Tracker.LEASE_DURATION)) < 0 && !leaseholder.equals(group.getLeaseHolder())) {// Ignore stale updates, except refresh for current leaseholder. TODO test
            resp.complete(new Response(now, 1, "Lease request ignored (wrong candidate)")); // TODO error code
            return;
        }

        group.setLeaseHolder(leaseholder);
        group.setLease(from);

        assert group.validLease(from, leaseholder);

        // Safe to commit topology on leader election.
        group.setState(members, now);
        group.commitState(now, true);

        // Node is up to date - move to OPERATIONAL. Skip this step if lease is refreshed.
        if (maxLwm != null) {
            if (group.lwm.equals(maxLwm)) {
                group.state = Tracker.State.OPERATIONAL;
            } else {
                group.state = Tracker.State.CATCHINGUP;
            }
        }

        if (id().equals(leaseholder)) {
            LOGGER.log(Level.INFO, "I am the leasholder: [interval={0}:{1}, at={2}, nodeId={3}]", from, from.adjust(Tracker.LEASE_DURATION), now, nodeId);
        } else {
            LOGGER.log(Level.INFO, "Refresh leasholder: [interval={0}:{1}, at={2}], nodeId={3}", from, from.adjust(Tracker.LEASE_DURATION), now, nodeId);
        }

        assert group.validLease(now, leaseholder);

        resp.complete(new Response(now));
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

    // TODO copypaste, combine lease and holder.
    public @Nullable Timestamp getLease(String grpName) {
        Group group = groups.get(grpName);
        if (group == null)
            return null;

        Timestamp now = clock.now();

        Timestamp lease = group.getLease();

        if (lease != null && now.compareTo(lease.adjust(Tracker.LEASE_DURATION)) < 0)
            return group.getLease();

        return null;
    }

    /**
     * Replicates a Put.
     *
     * Safety node: this methods must be bound to a single thread.
     *
     * @param grp The group.
     * @param put The command.
     * @return The result.
     */
    public CompletableFuture<Timestamp> replicate(String grp, Object payload) {
        Group group = groups.get(grp);

        Timestamp now = clock.now(); // Used as tx id.
        if (!group.validLease(now, nodeId)) {
            return CompletableFuture.failedFuture(new IllegalStateException("Illegal lease"));
        }

        CompletableFuture<Timestamp> resFut = new CompletableFuture<>();

        // TODO implement batching by concatenating queue elements into single message.
        group.executorService.submit(() -> {
            Set<NodeId> nodeIds = group.getMembers();

            AtomicInteger completed = new AtomicInteger(0);
            AtomicBoolean localDone = new AtomicBoolean();

            UUID traceId = UUID.randomUUID();

            Set<Inflight> errs = new HashSet<>();
            Set<Inflight> succ = new HashSet<>();

            for (NodeId id : nodeIds) {
                Replicator replicator = group.replicators.get(id);
                if (replicator == null) {
                    replicator = new Replicator(Node.this, id, grp, top); // TODO do we need to pass top ?
                    group.replicators.put(id, replicator);
                }

                Request request = new Request(); // Creating the request copy is essential.
                request.setId(traceId);
                request.setTs(now);
                request.setSender(nodeId);
                request.setGrp(grp);
                request.setPayload(new Replicate(replicator.getLwm(), payload));

                Inflight inflight = replicator.send(request, payload instanceof Put);

                // This future is completed from node's worker thread
                inflight.ioFuture().whenCompleteAsync((resp, ex) -> {
                    if (resp != null)
                        clock().onResponse(resp.getTs());

                    int maj = nodeIds.size() / 2 + 1;

                    completed.incrementAndGet();

                    if (ex != null || resp.getReturn() != 0) {
                        errs.add(inflight);
                    } else {
                        succ.add(inflight);
                    }

                    if (id.equals(Node.this.nodeId))
                        localDone.set(true);

                    if (completed.get() >= maj) {
                        if (errs.size() > nodeIds.size() - maj) { // Can tolerate minority fails
                            // Can propagate LWM for errors if operation is failed.
                            for (Inflight i : errs) {
                                i.finish(Replicator.State.ROLLBACK);
                            }
                            for (Inflight i : succ) {
                                i.finish(Replicator.State.ROLLBACK);
                            }

                            LOGGER.log(Level.DEBUG, "Err ack ts={0} req={1} node={2}", inflight.ts(), traceId, id);
                            resFut.completeExceptionally(new Exception("Replication failure"));
                        } else if (localDone.get()) { // Needs local completion.
                            for (Inflight i : errs) {
                                i.finish(Replicator.State.ERROR);
                            }
                            for (Inflight i : succ) {
                                i.finish(Replicator.State.COMMIT);
                            }

                            LOGGER.log(Level.DEBUG, "Ok ack ts={0} req={1} node={2}", inflight.ts(), traceId, id);
                            resFut.complete(now);
                        } else {
                            LOGGER.log(Level.DEBUG, "Ack ts={0} req={1} node={2}", inflight.ts(), traceId, id);
                        }
                    } else {
                        LOGGER.log(Level.DEBUG, "Ack ts={0} req={1} node={2}", inflight.ts(), traceId, id);
                    }
                }, group.executorService);
            }
        });

        return resFut;
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

    public CompletableFuture<Integer> localGet(String grp, int key, Timestamp ts) {
        Group group = groups.get(grp);
        assert group != null;

        CompletableFuture<Integer> fut = new CompletableFuture<>();

        group.executorService.submit(() -> {
            // Put to wait queue.
            if (group.lwm.compareTo(ts) < 0) {
                group.pendingReads.put(ts, new Read(key, fut));
                return;
            } else {
                Integer val = group.store.get(key, ts);
                fut.complete(val);
            }
        });

        return fut;
    }

    public CompletableFuture<Timestamp> sync(String grp) {
        Group group = groups.get(grp);

        CompletableFuture<Timestamp> res = new CompletableFuture<>();

        group.executorService.submit(() -> {
            Timestamp now = clock.now();
            if (!group.validLease(now, nodeId)) {
                res.completeExceptionally(new IllegalStateException("Illegal lease"));
                return;
            }

            Set<NodeId> nodeIds = new HashSet<>(group.getMembers());

            AtomicInteger acks = new AtomicInteger(0);

            for (NodeId id : nodeIds) {
                Replicator replicator = group.replicators.get(id);
                if (replicator == null) {
                    replicator = new Replicator(Node.this, id, grp, top);
                    group.replicators.put(id, replicator);
                }

                // Handle idle propagation.
                if (replicator.inflights() == 0) {
                    replicator.setLwm(now);
                    group.setLwm(now);
                }

                // Handle local node.
                if (id.equals(nodeId)) {
                    int val = acks.incrementAndGet();
                    if (val == nodeIds.size())
                        res.complete(now);
                    continue;
                }

                Request r = new Request();
                r.setSender(nodeId);
                r.setGrp(grp);
                r.setTs(now); // Propagate ts in idle sync.
                r.setPayload(new Sync(replicator.getLwm()));

                // TODO needs timeout - not all nodes can respond.
                replicator.idleSync(r).thenAccept(resp -> {
                    int val = acks.incrementAndGet();
                    if (val == nodeIds.size())
                        res.complete(now);
                });
            }
        });

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

    private void callback(Timestamp now, Map<NodeId, Timestamp> lwms, NodeId nodeId, @Nullable Timestamp lwm, Set<NodeId> members, Timestamp from, Group group, NodeId candidate, CompletableFuture<Response> resp) {
        if (resp.isDone())
            return;

        lwms.put(nodeId, lwm);

        // Collect response from majority. TODO this can be refactored to a collectFromMajority abstraction.
        int majority = members.size() / 2 + 1;
        int tolerable = members.size() - majority;

        if (majority == members.size()) { // Handle special case for two-nodes groups. They operate in full sync and can tolerate the loss of one node to remain available.
            majority = 1;
            tolerable = 1;
        }

        assert majority + tolerable == members.size();

        if (lwms.size() >= majority) {
            int succ = 0;
            int err = 0;

            for (Timestamp value : lwms.values()) {
                if (value != null)
                    succ++;
                else
                    err++;
            }

            if (err > tolerable) {
                resp.complete(new Response(this.clock.now(), 1, "Cannot assign a leaseholder because group is not available (required " + majority + " alive nodes)"));
                return;
            }

            // Fail attempt if can't collect enough lwms.
            if (succ == majority) {
                Timestamp ts = Timestamp.min();

                // Find max.
                for (Timestamp value : lwms.values()) {
                    if (value != null) {
                        if (value.compareTo(ts) > 0)
                            ts = value;
                    }
                }

                // Candidate must be in the max list, otherwise fail attempt.
                for (Entry<NodeId, Timestamp> entry0 : lwms.entrySet()) {
                    if (entry0.getKey() == candidate && !entry0.getValue().equals(ts)) {
                        resp.complete(new Response(this.clock.now(), 1, "Cannot assign leaseholder because it is not up-to-date node"));
                        return;
                    }
                }

                LOGGER.log(Level.INFO, "Collected lwms: [group={0}, leaseholder={1}, max={2}]", group.getName(), candidate, ts);

                resp.complete(new Response(now));

                // Asynchronously propagate the lease.
                Request request = new Request();
                request.setGrp(group.getName());
                request.setTs(now);
                request.setPayload(new LeaseGranted(group.getName(), from, candidate, members, ts));

                // Asynchronously notify alive members.
                for (NodeId nodeId0 : members) {
                    if (top.getNode(nodeId0) == null)
                        continue;

                    client.send(nodeId0, request);
                }
            }
        }
    }
}
