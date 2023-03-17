package com.ascherbakoff.ai3.cluster;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.cluster.Tracker.State;
import com.ascherbakoff.ai3.replication.Replicator;
import com.ascherbakoff.ai3.table.MVKeyValueTable;
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

public class Group {
    private final String name;

    public Timestamp lwm = Timestamp.min(); // Replication data low watermark.

    private Map<NodeId, State> nodeState = new HashMap<>();

    Map<NodeId, Replicator> replicators = new HashMap<>();

    public MVKeyValueTable<Integer, Integer> table = new MVKeyValueTable<>();

    private Timestamp lease;

    public TreeMap<Timestamp, Read> pendingReads = new TreeMap<>();

    private @Nullable NodeId leaseHolder;

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
        return nodeState;
    }

    public void setState(NodeId nodeId, State state) {
        nodeState.put(nodeId, state);
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
        if (!nodeState.equals(group.nodeState)) {
            return false;
        }
        if (lease != null ? !lease.equals(group.lease) : group.lease != null) {
            return false;
        }
        if (leaseHolder != null ? !leaseHolder.equals(group.leaseHolder) : group.leaseHolder != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + nodeState.hashCode();
        result = 31 * result + (lease != null ? lease.hashCode() : 0);
        result = 31 * result + (leaseHolder != null ? leaseHolder.hashCode() : 0);
        return result;
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
            Integer val = table.get(e.getValue().getKey(), e.getKey());
            e.getValue().getFut().complete(val);
        }

        head.clear();
    }
}
