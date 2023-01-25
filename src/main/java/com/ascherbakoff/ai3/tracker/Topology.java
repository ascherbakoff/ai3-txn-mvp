package com.ascherbakoff.ai3.tracker;

import java.util.HashMap;
import java.util.Map;

public class Topology {
    private Map<NodeId, Node> nodeMap = new HashMap<>();

    public void addNode(Node node) {
        nodeMap.putIfAbsent(node.id(), node);
    }

    public Node getNode(NodeId nodeId) {
        return nodeMap.get(nodeId);
    }

    public Map<NodeId, Node> getNodeMap() {
        return nodeMap;
    }

    public void regiser(Node node) {
        nodeMap.putIfAbsent(node.id(), node);
    }
}
