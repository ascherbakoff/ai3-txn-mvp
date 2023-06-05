package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;

public class ReplicateResponse extends Response {
    private final long repCntr;
    private final Timestamp repTs;

    public ReplicateResponse(Timestamp ts, long repCntr, Timestamp repTs) {
        super(ts);
        this.repCntr = repCntr;
        this.repTs = repTs;
    }

    public long getRepCntr() {
        return repCntr;
    }

    public Timestamp getRepTs() {
        return repTs;
    }
}
