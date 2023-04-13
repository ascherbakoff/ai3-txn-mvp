package com.ascherbakoff.ai3.cluster;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.cluster.Tracker.State;
import com.ascherbakoff.ai3.replication.Replicator;
import com.ascherbakoff.ai3.table.MVKeyValueStore;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
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
    public @Nullable Set<NodeId> members;
    public State state = State.IDLE;

    // Replicators for this group. Filled only on leader.
    Map<NodeId, Replicator> replicators = new HashMap<>();

    // MV store.
    public MVKeyValueStore<Integer, Integer> store = new MVKeyValueStore<>();

    // Leaseholder state.
    private Timestamp lease;
    private @Nullable NodeId leaseHolder;
    private Timestamp activationTs; // A timestamp at which lease was activated or refreshed. Used to determine catchup range.All repl commands happening after activation will receive higher timestamps.

    // Read requests, waiting for LWM.
    public TreeMap<Timestamp, Read> pendingReads = new TreeMap<>();

    public Timestamp tmpLwm = Timestamp.min();

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

    public Timestamp getActivationTs() {
        return activationTs;
    }

    public void setActivationTs(Timestamp activationTs) {
        this.activationTs = activationTs;
    }

    public Set<NodeId> getMembers() {
        return members == null ? Collections.emptySet() : members;
    }

    public void setState(Set<NodeId> members) {
        this.members = new HashSet<>(members);
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
        if (state != null ? !state.equals(group.state) : group.state != null) {
            return false;
        }
        if (lwm != null ? !lwm.equals(group.lwm) : group.lwm != null) {
            return false;
        }
        if (members != null ? !members.equals(group.members) : group.members != null) {
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
        if (this.state == State.CATCHINGUP) { // In CU state we are tracking temporary lwm.
            if (lwm.compareTo(this.tmpLwm) <= 0) {
                return; // Ignore stale LWM. this is safe, because all updates up to lwm already replicated.
            }
            this.tmpLwm = lwm;
            return;
        }

        if (lwm.compareTo(this.lwm) <= 0) {
            return; // Ignore stale LWM. this is safe, because all updates up to lwm already replicated.
        }

        this.lwm = lwm;
        NavigableMap<Timestamp, Read> head = pendingReads.headMap(lwm, true);
        for (Entry<Timestamp, Read> e : head.entrySet()) {
            Integer val = store.get(e.getValue().getKey(), e.getKey());
            e.getValue().getFut().complete(val);
        }

        head.clear();
    }

    public int size() {
        return members.size();
    }

    public int majority() {
        return size() / 2 + 1;
    }
}
