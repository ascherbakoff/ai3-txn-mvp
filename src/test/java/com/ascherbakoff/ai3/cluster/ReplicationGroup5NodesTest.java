package com.ascherbakoff.ai3.cluster;

/**
 * TODO
 */
public class ReplicationGroup5NodesTest extends BasicReplicationTest {
    private static System.Logger LOGGER = System.getLogger(ReplicationGroup5NodesTest.class.getName());

    @Override
    protected void createCluster() {
        createCluster(5);
    }
}
