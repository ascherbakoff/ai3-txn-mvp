package com.ascherbakoff.ai3.tracker;

import java.util.List;

public class GroupDesc {
    private List<NodeId> nodeIds;

    private final int id;

    public GroupDesc(int id) {
        this.id = id;
    }

    public Integer id() {
        return id;
    }

    public void addNodeId(NodeId nodeId) {
        if (nodeIds.contains(nodeId))
            throw new IllegalArgumentException("Node already registered " + nodeId);

        nodeIds.add(nodeId);
    }

    public void removeNodeId(NodeId nodeId) {
        if (!nodeIds.contains(nodeId))
            throw new IllegalArgumentException("Node not found " + nodeId);

        nodeIds.remove(nodeId);
    }

    public List<NodeId> nodeIds() {
        return nodeIds;
    }
}
