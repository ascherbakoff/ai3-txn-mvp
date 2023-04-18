package com.ascherbakoff.ai3.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.util.BasicTest;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 */
public class VersionChainRowStoreTest extends BasicTest {
    private VersionChainRowStore<Tuple> store = new VersionChainRowStore<>();

    @Test
    public void testInsertGetCommit() {
        // Start txn 1.
        Tuple t1 = Tuple.create("name1", "id1@some.org");
        UUID txId1 = new UUID(0, 0);
        VersionChain<Tuple> head = store.insert(t1, txId1);
        assertNotNull(head);

        assertEquals(t1, store.get(head, txId1, null));
        assertEquals(txId1, head.txId);

        List<VersionChain<Tuple>> heads = store.scan(txId1).getAll();
        assertTrue(heads.size() == 1);
        assertEquals(t1, store.get(heads.get(0), txId1, null));

        Timestamp commitTs = clock.now();
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

        List<VersionChain<Tuple>> heads = store.scan(txId1).getAll();
        assertTrue(heads.size() == 1);
        assertEquals(t1, store.get(heads.get(0), txId1, null));

        Tuple t2 = Tuple.create("name2", "id2@some.org");

        store.update(head, t2, txId1);
        assertEquals(t2, store.get(head, txId1, null));

        Timestamp commitTs = clock.now();
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
        Timestamp commitTs1 = clock.now();
        store.commitWrite(rowId, commitTs1, txId1);

        Tuple t2 = Tuple.create("name2", "id2@some.org");
        UUID txId2 = new UUID(0, 1);
        store.update(rowId, t2, txId2);
        Timestamp commitTs2 = clock.now();
        store.commitWrite(rowId, commitTs2, txId2);

        Tuple t3 = Tuple.create("name3", "id3@some.org");
        UUID txId3 = new UUID(0, 2);
        store.update(rowId, t3, txId3);
        Timestamp commitTs3 = clock.now();
        store.abortWrite(rowId, txId3);

        Tuple t4 = Tuple.create("name4", "id4@some.org");
        UUID txId4 = new UUID(0, 3);
        store.update(rowId, t4, txId4);
        Timestamp commitTs4 = clock.now();
        store.commitWrite(rowId, commitTs4, txId4);

        assertEquals(t4, store.get(rowId, commitTs4, null));

        UUID txId5 = new UUID(0, 4);
        store.update(rowId, Tuple.TOMBSTONE, txId5);
        Timestamp commitTs5 = clock.now();
        store.commitWrite(rowId, commitTs5, txId5);

        assertSame(Tuple.TOMBSTONE, store.get(rowId, new UUID(0, 5), null));
        assertSame(Tuple.TOMBSTONE, store.get(rowId, commitTs5, null));
        assertSame(Tuple.TOMBSTONE, store.scan(commitTs5).getAll().get(0));
        assertEquals(t4, store.scan(commitTs4).getAll().get(0));
        assertEquals(t2, store.get(rowId, commitTs3, null));
        assertEquals(t2, store.scan(commitTs3).getAll().get(0));
        assertEquals(t2, store.get(rowId, commitTs2, null));
        assertEquals(t2, store.scan(commitTs2).getAll().get(0));
        assertEquals(t1, store.get(rowId, commitTs1, null));
        assertEquals(t1, store.scan(commitTs1).getAll().get(0));
    }

    @Test
    public void testUpdate() {
        // Start txn 1.
        Tuple t1 = Tuple.create("name1", "id1@some.org");
        UUID txId1 = new UUID(0, 0);

        VersionChain<Tuple> rowId = store.insert(t1, txId1);
        assertNotNull(rowId);
        assertEquals(t1, store.get(rowId, txId1, null));
    }

    @Test
    public void testGC() {
        Tuple t1 = Tuple.create("name", "id@some.org");
        int gen = 0;
        UUID txId0 = new UUID(0, gen++);

        VersionChain<Tuple> rowId = store.insert(t1, txId0);
        assertNotNull(rowId);
        assertEquals(t1, store.get(rowId, txId0, null));

        store.commitWrite(rowId, clock.now(), txId0);

        for (int i = 1; i < 100; i++) {
            UUID txId = new UUID(0, gen++);
            Tuple t = Tuple.create("name" + i, "id@some.org");
            assertEquals(t1, store.update(rowId, t, txId));
            store.commitWrite(rowId, clock.now(), txId);
            t1 = t;
        }

        VersionChain<Tuple> c = rowId;
        int cnt = 0;
        for (int i = 99; i > 99 - VersionChain.MAX_ALLOWED; i--) {
            assertEquals(Tuple.create("name" + i, "id@some.org"), c.value);
            c = c.next;
            cnt++;
        }

        assertEquals(VersionChain.MAX_ALLOWED, cnt);
        assertNull(c);
    }

    @Test
    public void testClone() {
        Tuple t1 = Tuple.create("name0", "id@some.org");
        UUID txId0 = new UUID(0, 0);
        VersionChain<Tuple> rowId = store.insert(t1, txId0);
        store.commitWrite(rowId, new Timestamp(0, 0), txId0);

        int cnt = 5;
        for (int i = 1; i < cnt; i++) {
            UUID txId = new UUID(0, i);
            Tuple t = Tuple.create("name" + i, "id@some.org");
            assertEquals(t1, store.update(rowId, t, txId));
            store.commitWrite(rowId, new Timestamp(0, i), txId);
            t1 = t;
        }

        //rowId.printVersionChainOldToNew();

        rowId.printVersionChain();

        VersionChain<Tuple> cloned = rowId.clone(null);

        cloned.printVersionChain();
        cloned.printVersionChainOldToNew();

        cloned = rowId.clone(new Timestamp(0, 1));

        cloned.printVersionChain();
        cloned.printVersionChainOldToNew();

        // TODO validate
    }

    @Test
    public void testMerge() {
        Tuple t1 = Tuple.create("name20", "id@some.org");
        UUID txId1 = new UUID(0, 20);
        VersionChain<Tuple> head = store.insert(t1, txId1);
        store.commitWrite(head, new Timestamp(0, 20), txId1);

        Tuple t2 = Tuple.create("name0", "id@some.org");
        UUID txId2 = new UUID(0, 0);
        VersionChain<Tuple> rowId = store.insert(t2, txId2);
        store.commitWrite(rowId, new Timestamp(0, 0), txId2);

        int cnt = 5;
        for (int i = 1; i < cnt; i++) {
            UUID txId = new UUID(0, i);
            Tuple t = Tuple.create("name" + i, "id@some.org");
            assertEquals(t2, store.update(rowId, t, txId));
            store.commitWrite(rowId, new Timestamp(0, i), txId);
            t2 = t;
        }

        rowId.end = new Timestamp(0, 20);

        head.merge(rowId);

        head.printVersionChain();
        head.printVersionChainOldToNew();

        assertEquals(6, head.cnt);
    }

    @Test
    public void testMerge2() {
        Tuple t1 = Tuple.create("name20", "id@some.org");
        UUID txId1 = new UUID(0, 20);
        VersionChain<Tuple> head = store.insert(t1, txId1);
        store.commitWrite(head, new Timestamp(0, 20), txId1);

        Tuple t2 = Tuple.create("name0", "id@some.org");
        UUID txId2 = new UUID(0, 0);
        VersionChain<Tuple> rowId = store.insert(t2, txId2);
        store.commitWrite(rowId, new Timestamp(0, 0), txId2);

        rowId.end = new Timestamp(0, 20);
        head.merge(rowId);

        head.printVersionChain();
        head.printVersionChainOldToNew();

        assertEquals(2, head.cnt);
    }
}

