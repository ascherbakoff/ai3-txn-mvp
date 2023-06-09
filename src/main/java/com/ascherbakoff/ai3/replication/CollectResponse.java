package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;

public class CollectResponse extends Response {
    private final Timestamp repTs;

    public CollectResponse(Timestamp repTs, Timestamp ts) {
        super(ts);
        this.repTs = repTs;
    }

    public Timestamp getRepTs() {
        return repTs;
    }
}
