package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;

public class Response {
    private Timestamp clock;
    public Response(Timestamp clock) {
        this.clock = clock;
    }

    public Timestamp getClock() {
        return clock;
    }
}
