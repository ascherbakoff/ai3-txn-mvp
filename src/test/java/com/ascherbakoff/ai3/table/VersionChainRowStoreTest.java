package com.ascherbakoff.ai3.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.lock.DeadlockPrevention;
import com.ascherbakoff.ai3.lock.LockTable;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 */
public class VersionChainRowStoreTest {
    private VersionChainRowStore<Tuple> store = new VersionChainRowStore<>(new LockTable(10, true, DeadlockPrevention.none()));

    @Test
    public void testInsertGetCommit() {
        // Start txn 1.
        Tuple t1 = Tuple.create("name1", "id1@some.org");
        UUID txId1 = new UUID(0, 0);
        VersionChain<Tuple> head = store.insert(t1, txId1);
        assertNotNull(head);

        assertEquals(t1, store.get(head, txId1, null));
        assertEquals(txId1, head.txId);

        List<Tuple> heads = store.scan(txId1).getAll();
        assertTrue(heads.size() == 1);
        assertEquals(t1, heads.get(0));

        Timestamp commitTs = Timestamp.now();
        store.commitWrite(head, commitTs, txId1);
        assertNull(head.txId);
        assertEquals(commitTs, head.begin);
        assertNull(head.end);
        assertEquals(t1, head.value);
    }

    @Test
    public void testInsertUpdateGetCommit() {
        // Start txn 1.
        Tuple t1 = Tuple.create("name1", "id1@some.org");
        UUID txId1 = new UUID(0, 0);
        VersionChain<Tuple> head = store.insert(t1, txId1);
        assertNotNull(head);

        assertEquals(t1, store.get(head, txId1, null));
        assertEquals(txId1, head.txId);

        List<Tuple> heads = store.scan(txId1).getAll();
        assertTrue(heads.size() == 1);
        assertEquals(t1, heads.get(0));

        Tuple t2 = Tuple.create("name2", "id2@some.org");

        store.update(head, t2, txId1);
        assertEquals(t2, store.get(head, txId1, null));

        Timestamp commitTs = Timestamp.now();
        store.commitWrite(head, commitTs, txId1);
        assertNull(head.txId);
        assertEquals(commitTs, head.begin);
        assertNull(head.end);
        assertEquals(t2, head.value);
    }

    @Test
    public void testInsertAbort() {
        // Start txn 1.
        Tuple t1 = Tuple.create("name1", "id1@some.org");
        UUID txId1 = new UUID(0, 0);
        VersionChain<Tuple> head = store.insert(t1, txId1);
        assertNotNull(head);

        store.abortWrite(head, txId1);
        assertNull(store.scan(txId1).next());
    }

    /**
     * Tests the following history:
     * <p>Tx1 at (0, 0) insert -> commit
     * <p>Tx2 at (0, 1) update -> commit
     * <p>Tx3 at (0, 2) update -> abort
     * <p>Tx4 at (0, 3) update -> commit
     * <p>Tx5 at (0, 4) remove -> commit
     */
    @Test
    public void testHistory() {
        Tuple t1 = Tuple.create("name1", "id1@some.org");
        UUID txId1 = new UUID(0, 0);
        VersionChain<Tuple> rowId = store.insert(t1, txId1);
        Timestamp commitTs1 = Timestamp.now();
        store.commitWrite(rowId, commitTs1, txId1);

        Tuple t2 = Tuple.create("name2", "id2@some.org");
        UUID txId2 = new UUID(0, 1);
        store.update(rowId, t2, txId2);
        Timestamp commitTs2 = Timestamp.now();
        store.commitWrite(rowId, commitTs2, txId2);

        Tuple t3 = Tuple.create("name3", "id3@some.org");
        UUID txId3 = new UUID(0, 2);
        store.update(rowId, t3, txId3);
        Timestamp commitTs3 = Timestamp.now();
        store.abortWrite(rowId, txId3);

        Tuple t4 = Tuple.create("name4", "id4@some.org");
        UUID txId4 = new UUID(0, 3);
        store.update(rowId, t4, txId4);
        Timestamp commitTs4 = Timestamp.now();
        store.commitWrite(rowId, commitTs4, txId4);

        assertEquals(t4, store.get(rowId, (Timestamp) null, null));

        UUID txId5 = new UUID(0, 4);
        store.update(rowId, null, txId5);
        Timestamp commitTs5 = Timestamp.now();
        store.commitWrite(rowId, commitTs5, txId5);

        assertEquals(null, store.get(rowId, (Timestamp) null, null));
        assertEquals(null, store.get(rowId, commitTs5, null));
        assertTrue(store.scan(commitTs5).getAll().isEmpty());
        assertEquals(t4, store.scan(commitTs4).getAll().get(0));
        assertEquals(t2, store.get(rowId, commitTs3, null));
        assertEquals(t2, store.scan(commitTs3).getAll().get(0));
        assertEquals(t2, store.get(rowId, commitTs2, null));
        assertEquals(t2, store.scan(commitTs2).getAll().get(0));
        assertEquals(t1, store.get(rowId, commitTs1, null));
        assertEquals(t1, store.scan(commitTs1).getAll().get(0));
    }
}

