package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.cluster.NodeId;
import java.util.UUID;

public class Request {
    private String grp;
    private Timestamp ts;
    private Timestamp lwm;
    private Command payload;
    private UUID id;

    public String getGrp() {
        return grp;
    }

    public void setGrp(String grp) {
        this.grp = grp;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public Timestamp getTs() {
        return ts;
    }

    public Timestamp getLwm() {
        return lwm;
    }

    public void setLwm(Timestamp lwm) {
        this.lwm = lwm;
    }

    public void setPayload(Command payload) {
        this.payload = payload;
    }

    public Command getPayload() {
        return payload;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public UUID getId() {
        return id;
    }

    public enum Type {
        SYNC, DATA
    }
}
