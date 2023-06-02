package com.ascherbakoff.ai3.cluster;

import com.ascherbakoff.ai3.clock.Clock;
import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.replication.Collect;
import com.ascherbakoff.ai3.replication.CollectResponse;
import com.ascherbakoff.ai3.replication.Inflight;
import com.ascherbakoff.ai3.replication.LeaseGranted;
import com.ascherbakoff.ai3.replication.LeaseProposed;
import com.ascherbakoff.ai3.replication.Put;
import com.ascherbakoff.ai3.replication.Replicate;
import com.ascherbakoff.ai3.replication.Replicator;
import com.ascherbakoff.ai3.replication.Request;
import com.ascherbakoff.ai3.replication.Response;
import com.ascherbakoff.ai3.replication.RpcClient;
import com.ascherbakoff.ai3.replication.Snapshot;
import com.ascherbakoff.ai3.replication.Sync;
import java.lang.System.Logger.Level;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
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

    public Node(NodeId nodeId, Topology top, Clock clock, String... grps) {
        this.nodeId = nodeId;
        this.top = top;
        this.clock = clock;
        this.client = new RpcClient(top);
        for (String grp : grps) {
            groups.put(grp, new Group(grp));
        }
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

        if (groups.get(request.getGrp()) == null) {
            resp.completeExceptionally(new Exception("Group " + request.getGrp() + " not exists"));
            return resp;
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

        Object data = replicate.getData();
        if (data instanceof Put) {
            grp.accept(request.getTs(), replicate, false);
            resp.complete(new Response(now));
        } else {
            resp.complete(new Response(now, 1, "Unsupported command"));
        }
    }

    public void visit(LeaseGranted lease, Request request, CompletableFuture<Response> resp) {
        grant(lease.name(), lease.from(), lease.candidate(), lease.members(), lease.getRepCntr(), resp);
    }

    public void visit(LeaseProposed lease, Request request, CompletableFuture<Response> resp) {
        propose(lease.name(), lease.from(), lease.members(), resp);
    }

    public void visit(Sync sync, Request request, CompletableFuture<Response> resp) {
        Group grp = groups.get(request.getGrp());
        // TODO fail if grp null.
        grp.setIdle(sync.getTimestamp());

        resp.complete(new Response(clock.now()));
    }

    public void visit(Collect collect, Request request, CompletableFuture<Response> resp) {
        Group grp = groups.get(request.getGrp());

        resp.complete(new CollectResponse(grp.getRepCntr(), clock.now()));
    }

    public void visit(Snapshot snapshot, Request request, CompletableFuture<Response> resp) {
//        Group grp = groups.get(request.getGrp());
//
//        resp.complete(new SnapshotResponse(clock.now(), grp.store.snapshot(snapshot.getLow(), snapshot.getHigh())));
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

        if (prev != null && leaseStart.compareTo(prev.adjust(Tracker.LEASE_DURATION))
                < 0) { // Ignore stale updates, except refresh for current leaseholder. TODO test
            if (!candidate.equals(group.getLeader())) {
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

                request.setPayload(new LeaseGranted(group.getName(), leaseStart, group.getLeader(), members, group.getRepCntr()));

                if (top.getNode(nodeId0) == null) {
                    continue;
                }

                if (nodeId0.equals(candidate)) {
                    request.getPayload()
                            .accept(Node.this, request, resp); // Process in-place. All subsequent repl command will use higher timestamps.
                } else {
                    client.send(nodeId0, request);
                }
            }

            return;
        }

        Map<NodeId, Long> cntrs = new ConcurrentHashMap<>();

        for (NodeId nodeId : members) {
            Request request = new Request();
            request.setGrp(grp);
            request.setTs(now);
            request.setPayload(new Collect());

            if (top.getNodeMap().get(nodeId) == null) {
                callback(now, cntrs, nodeId, -1L, members, leaseStart, group, candidate, resp);
            } else {
                client.send(nodeId, request).orTimeout(Replicator.TIMEOUT_SEC, TimeUnit.SECONDS).thenAccept(response -> {
                    clock.onResponse(response.getTs());
                    CollectResponse cr = (CollectResponse) response;

                    // TODO report cause.
                    callback(now, cntrs, nodeId, cr.getRepCntr(), members, leaseStart, group, candidate, resp);
                }).exceptionally(err -> {
                    callback(now, cntrs, nodeId, -1L, members, leaseStart, group, candidate, resp);
                    return null; // TODO report cause.
                });
            }
        }
    }

    private void grant(String grp, Timestamp from, NodeId leaseholder, Set<NodeId> members, long maxCntr,
            CompletableFuture<Response> resp) {
        Timestamp now = clock.now();

        Group group = this.groups.get(grp);
        assert group != null;

        Timestamp prev = group.getLease();

        if (prev != null && from.compareTo(prev) < 0) {// Ignore stale updates.
            resp.complete(new Response(now, 1, "Lease request ignored (outdated)")); // TODO error code
            return;
        }

        if (prev != null && from.compareTo(prev.adjust(Tracker.LEASE_DURATION)) < 0 && !leaseholder.equals(
                group.getLeader())) {// Ignore stale updates, except refresh for current leaseholder. TODO test
            resp.complete(new Response(now, 1, "Lease request ignored (wrong candidate)")); // TODO error code
            return;
        }

        group.setLeader(leaseholder);
        group.setLease(from);
        assert group.validLease(from, leaseholder);

        group.setState(members);

        // Node is up to date - move to OPERATIONAL. Skip this step if lease is refreshed.
//        if (group.getRepCntr() == maxCntr) {
//            group.state = Tracker.State.OPERATIONAL;
//        } else {
//            group.state = Tracker.State.CATCHINGUP;
//            assert !leaseholder.equals(nodeId) : "Catching up node can't be leaseholder";
//        }

        if (id().equals(leaseholder)) {
            LOGGER.log(Level.INFO, "I am the leader: [interval={0}:{1}, at={2}, nodeId={3}, loc={4}, grp={5}]", from,
                    from.adjust(Tracker.LEASE_DURATION), now, nodeId, group.getRepCntr(), maxCntr);
        } else {
            LOGGER.log(Level.INFO, "Set leader: [interval={0}:{1}, at={2}], nodeId={3}, loc={4}, grp={5}", from,
                    from.adjust(Tracker.LEASE_DURATION), now, nodeId, group.getRepCntr(), maxCntr);
        }

        resp.complete(new Response(now));
    }

    public Group group(String grp) {
        return groups.get(grp);
    }

    public Clock clock() {
        return clock;
    }

    public @Nullable NodeId getLeader(String grpName) {
        Group group = groups.get(grpName);
        if (group == null) {
            return null;
        }

        Timestamp now = clock.now();

        Timestamp lease = group.getLease();

        if (lease != null && now.compareTo(lease.adjust(Tracker.LEASE_DURATION)) < 0) {
            return group.getLeader();
        }

        return null;
    }

    // TODO copypaste, combine lease and holder.
    public @Nullable Timestamp getLease(String grpName) {
        Group group = groups.get(grpName);
        if (group == null) {
            return null;
        }

        Timestamp now = clock.now();

        Timestamp lease = group.getLease();

        if (lease != null && now.compareTo(lease.adjust(Tracker.LEASE_DURATION)) < 0) {
            return group.getLease();
        }

        return null;
    }

    /**
     * Replicates a Put.
     *
     * Safety node: this methods must be bound to a single thread.
     *
     * @param grp The group.
     * @param put The command.
     * @return The future which is completed successfully when a majority of nodes has finished the replication.
     */
    public CompletableFuture<Timestamp> replicate(String grp, Object payload) {
        Group group = groups.get(grp);

        CompletableFuture<Timestamp> resFut = new CompletableFuture<>();

        // TODO implement batching by concatenating queue elements into single message.
        group.executorService.submit(() -> {
            Timestamp now = clock.now(); // Used as tx id.
            if (!group.validLease(now, nodeId)) {
                resFut.completeExceptionally(new IllegalStateException("Illegal lease"));
                return;
            }

            Set<NodeId> nodeIds = group.getMembers();

            AtomicInteger errs = new AtomicInteger();
            AtomicInteger succ = new AtomicInteger();

            final long cntr = group.nextCounter();
            final int maj = nodeIds.size() / 2 + 1;

            // Process local node.
            AtomicBoolean localDone = new AtomicBoolean();
            Replicate replicate = new Replicate(cntr, payload);
            group.accept(now, replicate, true);

            if (maj == 1) {
                group.updateSafe();
                resFut.complete(now);
                return;
            }

            succ.incrementAndGet(); // TODO async local processing. Step down if a leader fails to apply update.
            localDone.set(true);
            LOGGER.log(Level.INFO, "Local ack cntr={0} ts={1} node={2} sucs={3} errs={4} maj={5} done={6} err={7}",
                    cntr, now, nodeId, succ.get(), errs.get(), maj, resFut.isDone(), resFut.isCompletedExceptionally());

            for (NodeId id : nodeIds) {
                if (nodeId.equals(id)) {
                    continue;
                }

                Replicator replicator = createReplicator(grp, id, cntr - 1);

                Request request = new Request(); // Creating the request copy is essential.
                request.setTs(now);
                request.setSender(nodeId);
                request.setGrp(grp);
                request.setPayload(replicate);

                Inflight inflight = replicator.send(request);

                // This future is completed from node's worker thread
                Replicator finalReplicator = replicator;
                inflight.ioFuture().whenCompleteAsync((resp, ex) -> {
                    if (resp != null) {
                        clock().onResponse(resp.getTs());
                    }

                    if (ex != null || resp.getReturn() != 0) {
                        errs.incrementAndGet();
                    } else {
                        succ.incrementAndGet();
                    }

                    finalReplicator.fold();

                    if (succ.get() + errs.get() >= maj) {
                        if (errs.get() > nodeIds.size() - maj) { // Can tolerate minority fails
                            resFut.completeExceptionally(new Exception("Replication failure"));
                        } else if (localDone.get()) { // Needs local completion.
                            group.updateSafe();
                            resFut.complete(now);
                        }
                    }

                    LOGGER.log(Level.INFO, "Received ack cntr={0} ts={1} node={2} sucs={3} errs={4} maj={5} done={6} err={7}",
                            inflight.getReplicate().getCntr(), inflight.ts(), id, succ.get(), errs.get(), maj, resFut.isDone(),
                            resFut.isCompletedExceptionally());
                }, group.executorService);
            }
        });

        return resFut;
    }

    public Replicator createReplicator(String grp, NodeId id, long cntr) {
        Group group = groups.get(grp);

        Replicator replicator = group.replicators.get(id);
        if (replicator == null) {
            replicator = new Replicator(this, id, grp, top, cntr); // TODO do we need to pass top ?
            group.replicators.put(id, replicator);
        }

        return replicator;
    }

    public Replicator getReplicator(String grp, NodeId id) {
        Group group = groups.get(grp);

        return group.replicators.get(id);
    }

    public CompletableFuture<Void> catchUp(String grpName) {
        Group group = groups.get(grpName);
        assert group != null;
        //assert group.state == Tracker.State.CATCHINGUP;

        NodeId leaseHolder = getLeader(grpName);
        if (leaseHolder == null) {
            return CompletableFuture.failedFuture(new Exception("Invalid lease"));
        }

        LOGGER.log(Level.INFO, "Catching up [grp={0}, missed={1}:{2}, leader={3}]", grpName, group.getRepCntr(), 0,
                leaseHolder);

//        Request request = new Request();
//        request.setId(UUID.randomUUID());
//        request.setTs(clock.now());
//        request.setGrp(grpName);
//        request.setPayload(new Snapshot(group.repTs, group.getActivationTs()));
//        return client.send(leaseHolder, request).thenApply(resp -> {
//            SnapshotResponse snapResp = (SnapshotResponse) resp;
//
//            VersionChainRowStore<Entry<Integer, Integer>> snapshot = snapResp.getSnapshot();
//
//            Timestamp old = group.repTs;
//
//            // TODO make async
//            group.setSnapshot(snapshot);
//
//            LOGGER.log(Level.INFO, "Catch up finished [grp={0}, missed={1}:{2}, leader={3}]", grpName, old, group.getActivationTs(),
//                    leaseHolder);
//
//            return null;
//        });

        return null;
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
            if (group.getRepTs().compareTo(ts) < 0) {
                group.pendingReads.put(ts, new Read(key, fut));
                return;
            } else {
                Integer val = 0; // group.snapIdx.headMap(key, true);
                fut.complete(val);
            }
        });

        return fut;
    }

    public Future<Timestamp> sync(String grp) {
        Group group = groups.get(grp);

        return group.executorService.submit(new Callable<Timestamp>() {
            @Override
            public Timestamp call() throws Exception {
                Timestamp now = clock.now();
                if (!group.validLease(now, nodeId)) {
                    throw new IllegalStateException("Illegal lease");
                }

                Set<NodeId> nodeIds = new HashSet<>(group.getMembers());

                for (NodeId id : nodeIds) {
                    // Handle local node.
                    if (id.equals(nodeId)) {
                        group.setRepTs(now); // TODO leader can use current ts as repTs.
                        continue;
                    }

                    Replicator replicator = createReplicator(grp, id, group.getRepCntr());

                    if (replicator.inflights() != 0) {
                        continue;
                    }

                    Request r = new Request();
                    r.setSender(nodeId);
                    r.setGrp(grp);
                    r.setTs(now); // Propagate ts in idle sync.
                    r.setPayload(new Sync(now)); // TODO remove

                    // TODO needs timeout - not all nodes can respond.
                    replicator.idleSync(r);
                }

                return now;
            }
        });
    }

    enum State {
        STARTED, STOPPED
    }

//    private static class TrackerState {
//        final Timestamp last;
//
//        final NodeId leaseholder;
//
//        final Map<NodeId, Tracker.State> nodeState;
//
//        TrackerState(Timestamp last, @Nullable NodeId leaseholder, Map<NodeId, Tracker.State> nodeState) {
//            this.last = last;
//            this.leaseholder = leaseholder;
//            this.nodeState = nodeState;
//        }
//    }

    private void callback(Timestamp now, Map<NodeId, Long> cntrs, NodeId nodeId, Long cntr, Set<NodeId> members, Timestamp from,
            Group group, NodeId candidate, CompletableFuture<Response> resp) {
        LOGGER.log(Level.INFO, "Received response [group={0}, leaseholder={1}, cntr={2}, node={3}]", group.getName(), candidate, cntr, nodeId);

        if (resp.isDone()) {
            return;
        }

        cntrs.put(nodeId, cntr);

        // Collect response from majority. TODO this can be refactored to a collectFromMajority abstraction.
        int majority = members.size() / 2 + 1;
        int tolerable = members.size() - majority;

        // Handle special case for two-nodes groups. They operate in full sync and can tolerate the loss of one node to remain available.
        if (majority == members.size()) {
            assert majority == 2;
            majority = 1;
            tolerable = 1;
        }

        assert majority + tolerable == members.size();

        if (cntrs.size() >= majority) {
            int succ = 0;
            int err = 0;

            for (Long value : cntrs.values()) {
                if (value != -1) {
                    succ++;
                } else {
                    err++;
                }
            }

            if (err > tolerable) {
                resp.complete(new Response(this.clock.now(), 1,
                        "Cannot assign a leaseholder because group is not available (required " + majority + " alive nodes)"));
                return;
            }

            // Fail attempt if can't collect enough lwms.
            if (succ >= majority) {
                long maxCntr = 0;

                // Find max.
                for (Long value : cntrs.values()) {
                    if (value > maxCntr) {
                        maxCntr = value;
                    }
                }

                // Candidate must be in the max list, otherwise fail attempt.
                for (Entry<NodeId, Long> entry0 : cntrs.entrySet()) {
                    if (entry0.getKey() == candidate && !entry0.getValue().equals(maxCntr)) {
                        resp.complete(new Response(this.clock.now(), 1, "Cannot assign leaseholder because it is not up-to-date node"));
                        return;
                    }
                }

                if (resp.complete(new Response(now))) {
                    LOGGER.log(Level.INFO, "Collected majority of counters: [group={0}, leaseholder={1}, max={2}]", group.getName(), candidate, maxCntr);
                    // Asynchronously propagate the lease.
                    Request request = new Request();
                    request.setGrp(group.getName());
                    request.setTs(now);
                    request.setPayload(new LeaseGranted(group.getName(), from, candidate, members, maxCntr));

                    // Asynchronously notify alive members.
                    for (NodeId nodeId0 : members) {
                        if (top.getNode(nodeId0) == null) {
                            continue;
                        }

                        client.send(nodeId0, request);
                    }
                }
            }
        }
    }
}
