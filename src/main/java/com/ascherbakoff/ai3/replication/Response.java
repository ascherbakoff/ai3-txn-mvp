package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;

public class Response {
    private Timestamp ts;
    public Response(Timestamp ts) {
        this.ts = ts;
    }

    public Timestamp getTs() {
        return ts;
    }
}
