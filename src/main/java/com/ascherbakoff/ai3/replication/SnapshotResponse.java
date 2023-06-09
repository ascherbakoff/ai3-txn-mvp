package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;
import java.util.TreeMap;
import org.jetbrains.annotations.Nullable;

public class SnapshotResponse extends Response {
    private final TreeMap<Timestamp, Replicate> snapshot;
    private final Timestamp current;

    /**
     * Null "current" means a stable replication.
     *
     * @param hlc
     * @param snapshot
     * @param current
     */
    public SnapshotResponse(Timestamp hlc, TreeMap<Timestamp, Replicate> snapshot, @Nullable Timestamp current) {
        super(hlc);
        this.snapshot = snapshot;
        this.current = current;
    }

    public TreeMap<Timestamp, Replicate> getSnapshot() {
        return snapshot;
    }

    public @Nullable Timestamp getCurrent() {
        return current;
    }
}
