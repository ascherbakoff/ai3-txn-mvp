package com.ascherbakoff.ai3.cluster;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.cluster.Tracker.State;
import com.ascherbakoff.ai3.replication.Replicator;
import com.ascherbakoff.ai3.table.MVKeyValueStore;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Local replication group.
 */
public class Group {
    private final String name;

    // Replication data low watermark.
    public Timestamp lwm = Timestamp.min();

    // Group state.
    public @Nullable Timestamp pendingEpoch;
    public @Nullable Timestamp epoch;
    public @Nullable Map<NodeId, State> pendingNodeState;
    public @Nullable Map<NodeId, State> nodeState;

    // Replicators for this group. Filled only on leader.
    Map<NodeId, Replicator> replicators = new HashMap<>();

    // MV store.
    public MVKeyValueStore<Integer, Integer> store = new MVKeyValueStore<>();

    // Leaseholder state.
    private Timestamp lease;
    private @Nullable NodeId leaseHolder;

    // Read requests, waiting for LWM.
    public TreeMap<Timestamp, Read> pendingReads = new TreeMap<>();

    public Group(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public @Nullable Timestamp getLease() {
        return lease;
    }

    public void setLease(Timestamp lease) {
        this.lease = lease;
    }

    public NodeId getLeaseHolder() {
        return leaseHolder;
    }

    public void setLeaseHolder(NodeId leaseHolder) {
        this.leaseHolder = leaseHolder;
    }

    public Map<NodeId, State> getNodeState() {
        return nodeState == null ? Collections.emptyMap() : nodeState;
    }

    public void setState(Map<NodeId, State> nodeState, Timestamp from) {
        this.pendingNodeState = new HashMap<>(nodeState);
        this.pendingEpoch = from;
    }

    public boolean commitState(Timestamp from, boolean finish) {
        if (!from.equals(pendingEpoch))
            return false;

        if (finish) {
            nodeState = pendingNodeState;
            epoch = pendingEpoch;
        } else {
            pendingNodeState = null;
            pendingEpoch = null;
        }

        return true;
    }

    public ExecutorService executorService = Executors.newSingleThreadExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(@NotNull Runnable r) {
            Thread thread = new Thread(r);
            thread.setName(name + "-sender-thread");
            return thread;
        }
    });

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Group group = (Group) o;

        if (!name.equals(group.name)) {
            return false;
        }
        if (lwm != null ? !lwm.equals(group.lwm) : group.lwm != null) {
            return false;
        }
        if (pendingEpoch != null ? !pendingEpoch.equals(group.pendingEpoch) : group.pendingEpoch != null) {
            return false;
        }
        if (epoch != null ? !epoch.equals(group.epoch) : group.epoch != null) {
            return false;
        }
        if (pendingNodeState != null ? !pendingNodeState.equals(group.pendingNodeState) : group.pendingNodeState != null) {
            return false;
        }
        if (nodeState != null ? !nodeState.equals(group.nodeState) : group.nodeState != null) {
            return false;
        }
        if (lease != null ? !lease.equals(group.lease) : group.lease != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    public boolean validLease(Timestamp at, NodeId leaseHolder) {
        if (!leaseHolder.equals(this.leaseHolder)) // Take into account the case of no leaseholder at all.
            return false;

        assert lease != null;

        return lease.compareTo(at) <= 0 && at.compareTo(lease.adjust(Tracker.LEASE_DURATION)) < 0 ? true : false;
    }

    public void setLwm(Timestamp lwm) {
        this.lwm = lwm;
        NavigableMap<Timestamp, Read> head = pendingReads.headMap(lwm, true);
        for (Entry<Timestamp, Read> e : head.entrySet()) {
            Integer val = store.get(e.getValue().getKey(), e.getKey());
            e.getValue().getFut().complete(val);
        }

        head.clear();
    }

    public int size() {
        return nodeState.size();
    }

    public int majority() {
        return size() / 2 + 1;
    }
}
