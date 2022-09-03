package com.ascherbakoff.ai3.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ascherbakoff.ai3.clock.Timestamp;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 */
public class VersionChainRowStoreTest {
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

//    @Test
//    public void testInsertUnique() {
//        VersionChainRowStore store = new VersionChainRowStore();
//
//        // Start txn 1.
//        Tuple t1 = Tuple.create("name1", "id1@some.org");
//        UUID txId1 = new UUID(0, 0);
//        VersionChain<Tuple> head = store.insert(t1, txId1);
//        assertNotNull(head);
//        store.commitWrite(head, Timestamp.now(), txId1);
//
//        // Start txn 2.
//        Tuple t2 = Tuple.create().set("name", "name1").set("email", "id1@some.org");
//        assertThrows(IllegalArgumentException.class, () -> store.insert(t2, Timestamp.nextUUID()));
//    }
//
//    @Test
//    public void testInsertNonUnique() {
//        VersionChainRowStore store = new VersionChainRowStore(Map.of("email", new HashIndexImpl("email", false)));
//
//        // Start txn 1.
//        Tuple t1 = Tuple.create().set("name", "name1").set("email", "id1@some.org");
//        UUID txId1 = Timestamp.nextUUID();
//        VersionChain<Tuple> head = store.insert(t1, txId1);
//        assertNotNull(head);
//        Timestamp commitTs = Timestamp.nextVersion();
//        store.commit(head, txId1, commitTs);
//
//        // Start txn 2.
//        Tuple t2 = Tuple.create().set("name", "name2").set("email", "id1@some.org");
//        UUID txId2 = Timestamp.nextUUID();
//        VersionChain<Tuple> head2 = store.insert(t2, txId2);
//        assertNotNull(head2);
//        Timestamp commitTs2 = Timestamp.nextVersion();
//        store.commit(head2, txId2, commitTs2);
//
//        // Validate.
//        validate(store, "id1@some.org", head, head2, commitTs2);
//        validate(store, "id1@some.org", head, head2, null);
//    }
//
//    @Test
//    public void testInsertInsertMergeUnique() {
//        VersionChainRowStore store = new VersionChainRowStore(Map.of("email", new HashIndexImpl("email", true)));
//
//        // Start txn 1: insert row 1.
//        Tuple t1 = Tuple.create().set("name", "name1").set("email", "id1@some.org");
//        UUID txId1 = Timestamp.nextUUID();
//        VersionChain<Tuple> head = store.insert(t1, txId1);
//        assertNotNull(head);
//        Timestamp commitTs = Timestamp.nextVersion();
//        store.commit(head, txId1, commitTs);
//
//        // Start txn 2: insert row 2.
//        Tuple t2 = Tuple.create().set("name", "name1").set("email", "id2@some.org");
//        UUID txId2 = Timestamp.nextUUID();
//        VersionChain<Tuple> head2 = store.insert(t2, txId2);
//        assertNotNull(head);
//        Timestamp commitTs2 = Timestamp.nextVersion();
//        store.commit(head2, txId2, commitTs2);
//
//        // Start txn 3: update row 1 by changing email to existing value.
//        UUID txId3 = Timestamp.nextUUID();
//        Iterator<VersionChain<Tuple>> iter = store.scan("email", Tuple.create().set("email", "id1@some.org"));
//        assertTrue(iter.hasNext());
//        assertTrue(iter.next() == head); // Chain head is immutable.
//        assertFalse(iter.hasNext());
//        Tuple oldRow = store.resolve(head, null, null);
//        assertNotNull(oldRow);
//        Tuple newRow = Tuple.create(oldRow).set("email", "id2@some.org");
//
//        assertThrows(IllegalArgumentException.class, () -> store.merge(head, oldRow, newRow, txId3));
//    }
//
//    @Test
//    public void testInsertInsertMergeNonUnique() {
//        VersionChainRowStore store = new VersionChainRowStore(Map.of("email", new HashIndexImpl("email", false)));
//
//        // Start txn 1: insert row 1.
//        Tuple t1 = Tuple.create().set("name", "name1").set("email", "id1@some.org");
//        UUID txId1 = Timestamp.nextUUID();
//        VersionChain<Tuple> head = store.insert(t1, txId1);
//        assertNotNull(head);
//        Timestamp commitTs = Timestamp.nextVersion();
//        store.commit(head, txId1, commitTs);
//
//        // Start txn 2: insert row 2.
//        Tuple t2 = Tuple.create().set("name", "name1").set("email", "id2@some.org");
//        UUID txId2 = Timestamp.nextUUID();
//        VersionChain<Tuple> head2 = store.insert(t2, txId2);
//        assertNotNull(head);
//        Timestamp commitTs2 = Timestamp.nextVersion();
//        store.commit(head2, txId2, commitTs2);
//
//        // Start txn 3: update row 1 by changing email to existing value.
//        UUID txId3 = Timestamp.nextUUID();
//        Iterator<VersionChain<Tuple>> iter = store.scan("email", Tuple.create().set("email", "id1@some.org"));
//        assertTrue(iter.hasNext());
//        assertTrue(iter.next() == head); // Chain head is immutable.
//        assertFalse(iter.hasNext());
//        Tuple oldRow = store.resolve(head, null, null);
//        assertNotNull(oldRow);
//        Tuple newRow = Tuple.create(oldRow).set("email", "id2@some.org");
//        store.merge(head, oldRow, newRow, txId3);
//        Timestamp commitTs3 = Timestamp.nextVersion();
//        store.commit(head, txId3, commitTs3);
//
//        validate(store, "id2@some.org", head, head2, commitTs3);
//    }
//
//    @Test
//    public void testInsertMergeInsertUnique() {
//        VersionChainRowStore store = new VersionChainRowStore(Map.of("email", new HashIndexImpl("email", true)));
//
//        // Start txn 1: insert row 1.
//        Tuple t1 = Tuple.create().set("name", "name1").set("email", "id1@some.org");
//        UUID txId1 = Timestamp.nextUUID();
//        VersionChain<Tuple> head = store.insert(t1, txId1);
//        assertNotNull(head);
//        Timestamp commitTs = Timestamp.nextVersion();
//        store.commit(head, txId1, commitTs);
//
//        // Start txn 2: update row 1 by changing email.
//        Predicate<Tuple> pred = t -> "id1@some.org".equals(t.valueOrDefault("email", null));
//        UUID txId2 = Timestamp.nextUUID();
//        Timestamp commitTs2 = Timestamp.nextVersion();
//        Iterator<VersionChain<Tuple>> iter = store.scan("email", Tuple.create().set("email", "id1@some.org")); // TODO acquire locks on index.
//        assertTrue(iter.hasNext());
//        assertTrue(iter.next() == head); // Chain head is immutable.
//        assertFalse(iter.hasNext());
//        Tuple oldRow = store.resolve(head, null, pred); // TODO acquire lock on rowId.
//        assertNotNull(oldRow);
//        Tuple newRow = Tuple.create(oldRow).set("email", "id2@some.org");
//        store.merge(head, oldRow, newRow, txId2);
//        store.commit(head, txId2, commitTs2);
//
//        iter = store.scan("email", Tuple.create().set("email", "id2@some.org"));
//        assertTrue(iter.hasNext());
//        assertEquals("id2@some.org", store.resolve(head, null, null).valueOrDefault("email", null));
//
//        iter = store.scan("email", Tuple.create().set("email", "id1@some.org"));
//        assertTrue(iter.hasNext());
//        assertEquals("id1@some.org", store.resolve(head, commitTs, null).valueOrDefault("email", null));
//
//        // Start txn 3: insert row 2 with existing email.
//        Tuple t3 = Tuple.create().set("name", "name3").set("email", "id2@some.org");
//        UUID txId3 = Timestamp.nextUUID();
//        assertThrows(IllegalArgumentException.class, () -> store.insert(t3, txId3));
//    }
//
//    @Test
//    public void testInsertMergeInsertUnique2() {
//        VersionChainRowStore store = new VersionChainRowStore(Map.of("email", new HashIndexImpl("email", true)));
//
//        // Start txn 1: insert row 1.
//        Tuple t1 = Tuple.create().set("name", "name1").set("email", "id1@some.org");
//        UUID txId1 = Timestamp.nextUUID();
//        VersionChain<Tuple> head = store.insert(t1, txId1);
//        assertNotNull(head);
//        Timestamp commitTs = Timestamp.nextVersion();
//        store.commit(head, txId1, commitTs);
//
//        // Start txn 2: update row 1 by changing email. This will produce new row version and two index records.
//        Predicate<Tuple> pred = t -> "id1@some.org".equals(t.valueOrDefault("email", null));
//        UUID txId2 = Timestamp.nextUUID();
//        Timestamp commitTs2 = Timestamp.nextVersion();
//        Iterator<VersionChain<Tuple>> iter = store.scan("email", Tuple.create().set("email", "id1@some.org")); // TODO acquire locks on index.
//        assertTrue(iter.hasNext());
//        assertTrue(iter.next() == head); // Chain head is immutable.
//        assertFalse(iter.hasNext());
//        Tuple oldRow = store.resolve(head, null, pred); // TODO acquire lock on rowId.
//        assertNotNull(oldRow);
//        Tuple newRow = Tuple.create(oldRow).set("email", "id2@some.org");
//        store.merge(head, oldRow, newRow, txId2);
//        store.commit(head, txId2, commitTs2);
//
//        // Start txn 3: insert row 2 with existing email, but in earlier history.
//        Tuple t3 = Tuple.create().set("name", "name3").set("email", "id1@some.org");
//        UUID txId3 = Timestamp.nextUUID();
//        VersionChain<Tuple> head3 = store.insert(t3, txId3);
//        assertNotNull(head3);
//        Timestamp commitTs3 = Timestamp.nextVersion();
//        store.commit(head3, txId3, commitTs3);
//
//        // Validate.
//        iter = store.scan("email", Tuple.create().set("email", "id1@some.org"));
//
//        Set<VersionChain<Tuple>> rowIds = new HashSet<>();
//
//        while (iter.hasNext()) {
//            VersionChain<Tuple> next = iter.next();
//            rowIds.add(next);
//        }
//
//        assertEquals(2, rowIds.size());
//        assertTrue(rowIds.contains(head));
//        assertTrue(rowIds.contains(head3));
//
//        Timestamp readTs = commitTs3;  // Can use null for timestamp.
//
//        assertNull(store.resolve(head, readTs, pred));
//        assertEquals("id1@some.org", store.resolve(head3, readTs, pred).valueOrDefault("email", null));
//    }
//
//    @Test
//    public void testInsertMergeInsertNonUnique() {
//        VersionChainRowStore store = new VersionChainRowStore(Map.of("email", new HashIndexImpl("email", false)));
//
//        // Start txn 1: insert row 1.
//        Tuple t1 = Tuple.create();
//        t1.set("name", "name1");
//        t1.set("email", "id1@some.org");
//        UUID txId1 = Timestamp.nextUUID();
//        VersionChain<Tuple> head = store.insert(t1, txId1);
//        assertNotNull(head);
//        Timestamp commitTs = Timestamp.nextVersion();
//        store.commit(head, txId1, commitTs);
//
//        // Start txn 2: update row 1 by changing email.
//        Predicate<Tuple> pred = t -> "id1@some.org".equals(t.valueOrDefault("email", null));
//        UUID txId2 = Timestamp.nextUUID();
//        Timestamp commitTs2 = Timestamp.nextVersion();
//        Iterator<VersionChain<Tuple>> iter = store.scan("email", Tuple.create().set("email", "id1@some.org")); // TODO acquire locks on index.
//        assertTrue(iter.hasNext());
//        assertTrue(iter.next() == head); // Chain head is immutable.
//        assertFalse(iter.hasNext());
//        Tuple oldRow = store.resolve(head, null, pred); // TODO acquire lock on rowId.
//        assertNotNull(oldRow);
//        Tuple newRow = Tuple.create(oldRow).set("email", "id2@some.org");
//        store.merge(head, oldRow, newRow, txId2);
//        store.commit(head, txId2, commitTs2);
//
//        // Start txn 3.
//        Tuple t3 = Tuple.create().set("name", "name3").set("email", "id2@some.org");
//        UUID txId3 = Timestamp.nextUUID();
//        VersionChain<Tuple> head3 = store.insert(t3, txId3);
//        assertNotNull(head3);
//        Timestamp commitTs3 = Timestamp.nextVersion();
//        store.commit(head3, txId3, commitTs3);
//
//        iter = store.scan("email", Tuple.create().set("email", "id2@some.org"));
//
//        Set<VersionChain<Tuple>> rowIds = new HashSet<>();
//
//        while (iter.hasNext()) {
//            VersionChain<Tuple> next = iter.next();
//            rowIds.add(next);
//        }
//
//        assertEquals(2, rowIds.size());
//        assertTrue(rowIds.contains(head));
//        assertTrue(rowIds.contains(head3));
//
//        Timestamp readTs = commitTs3;  // Can use null for timestamp.
//
//        Predicate<Tuple> pred2 = t -> "id2@some.org".equals(t.valueOrDefault("email", null));
//        assertEquals("id2@some.org", store.resolve(head, readTs, pred2).valueOrDefault("email", null));
//        assertEquals("id2@some.org", store.resolve(head3, readTs, pred2).valueOrDefault("email", null));
//    }
//
//    @Test
//    public void testInsertMergeInsertNonUnique2() {
//        VersionChainRowStore store = new VersionChainRowStore(Map.of("email", new HashIndexImpl("email", false)));
//
//        // Start txn 1: insert row 1.
//        Tuple t1 = Tuple.create();
//        t1.set("name", "name1");
//        t1.set("email", "id1@some.org");
//        UUID txId1 = Timestamp.nextUUID();
//        VersionChain<Tuple> head = store.insert(t1, txId1);
//        assertNotNull(head);
//        Timestamp commitTs = Timestamp.nextVersion();
//        store.commit(head, txId1, commitTs);
//
//        // Start txn 2: update row 1 by changing email.
//        Predicate<Tuple> pred = t -> "id1@some.org".equals(t.valueOrDefault("email", null));
//        UUID txId2 = Timestamp.nextUUID();
//        Timestamp commitTs2 = Timestamp.nextVersion();
//        Iterator<VersionChain<Tuple>> iter = store.scan("email", Tuple.create().set("email", "id1@some.org")); // TODO acquire locks on index.
//        assertTrue(iter.hasNext());
//        assertTrue(iter.next() == head); // Chain head is immutable.
//        assertFalse(iter.hasNext());
//        Tuple oldRow = store.resolve(head, null, pred); // TODO acquire lock on rowId.
//        assertNotNull(oldRow);
//        Tuple newRow = Tuple.create(oldRow).set("email", "id2@some.org");
//        store.merge(head, oldRow, newRow, txId2);
//        store.commit(head, txId2, commitTs2);
//
//        // Start txn 3.
//        Tuple t3 = Tuple.create().set("name", "name3").set("email", "id1@some.org");
//        UUID txId3 = Timestamp.nextUUID();
//        VersionChain<Tuple> head3 = store.insert(t3, txId3);
//        assertNotNull(head3);
//        Timestamp commitTs3 = Timestamp.nextVersion();
//        store.commit(head3, txId3, commitTs3);
//
//        iter = store.scan("email", Tuple.create().set("email", "id1@some.org"));
//
//        Set<VersionChain<Tuple>> rowIds = new HashSet<>();
//
//        while (iter.hasNext()) {
//            VersionChain<Tuple> next = iter.next();
//            rowIds.add(next);
//        }
//
//        assertEquals(2, rowIds.size());
//        assertTrue(rowIds.contains(head));
//        assertTrue(rowIds.contains(head3));
//
//        Timestamp readTs = commitTs3;  // Can use null for timestamp.
//
//        assertNull(store.resolve(head, readTs, pred));
//        assertEquals("id1@some.org", store.resolve(head3, readTs, pred).valueOrDefault("email", null));
//    }
//
//    private void validate(VersionChainRowStore store, String email, VersionChain<Tuple> head, VersionChain<Tuple> head2, Timestamp commitTs) {
//        Iterator<VersionChain<Tuple>> iter = store.scan("email", Tuple.create().set("email", email));
//
//        Set<VersionChain<Tuple>> rowIds = new HashSet<>();
//
//        while (iter.hasNext()) {
//            VersionChain<Tuple> next = iter.next();
//            rowIds.add(next);
//        }
//
//        assertEquals(2, rowIds.size());
//        assertTrue(rowIds.contains(head));
//        assertTrue(rowIds.contains(head2));
//
//        Predicate<Tuple> pred = t -> email.equals(t.valueOrDefault("email", null));
//        assertEquals(email, store.resolve(head, commitTs, pred).valueOrDefault("email", null));
//        assertEquals(email, store.resolve(head2, commitTs, pred).valueOrDefault("email", null));
//    }
}

