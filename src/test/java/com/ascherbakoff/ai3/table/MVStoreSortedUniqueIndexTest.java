package com.ascherbakoff.ai3.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.lock.DeadlockPrevention;
import com.ascherbakoff.ai3.lock.LockTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

public class MVStoreSortedUniqueIndexTest extends MVStoreBasicUniqueIndexTest {
    public MVStoreSortedUniqueIndexTest() {
        {
            VersionChainRowStore<Tuple> rowStore = new VersionChainRowStore<>();
            store = new MVStoreImpl(
                    rowStore,
                    new LockTable(10, true, DeadlockPrevention.none()),
                    Map.of(0, new SortedUniqueIndex(0, new LockTable(10, true, DeadlockPrevention.none()), new SortedIndexStoreImpl<>(), rowStore))
            );
        }
    }

    /**
     * Tests col in (0, 1, 2)
     */
    @Test
    public void testRangeQuerySingle() {
        UUID txId = new UUID(0, 0);

        store.insert(Tuple.create(0, "val0"), txId).join();
        store.insert(Tuple.create(1, "val1"), txId).join();
        store.insert(Tuple.create(2, "val2"), txId).join();

        assertEquals(Tuple.create(0, "val0"), getSingle(txId, 0, Tuple.create(0)));
        assertEquals(Tuple.create(1, "val1"), getSingle(txId, 0, Tuple.create(1)));
        assertEquals(Tuple.create(2, "val2"), getSingle(txId, 0, Tuple.create(2)));
        assertNull(getSingle(txId, 0, Tuple.create(3)));
    }

    /**
     * Tests range 0 <= col <= 4
     */
    @Test
    public void testRangeQuery() {
        UUID txId = new UUID(0, 0);

        insert(txId);

        List<VersionChain<Tuple>> rows =
                store.query(new RangeQuery(0, Tuple.create(0), true, Tuple.create(4), true), txId).loadAll(new ArrayList<>()).join();

        validateRows(rows, 3, 0, txId);
    }

    /**
     * Tests range 0 < col <= 4
     */
    @Test
    public void testRangeQuery2() {
        UUID txId = new UUID(0, 0);

        insert(txId);

        List<VersionChain<Tuple>> rows =
                store.query(new RangeQuery(0, Tuple.create(0), false, Tuple.create(4), true), txId).loadAll(new ArrayList<>()).join();

        validateRows(rows, 2, 2, txId);
    }

    /**
     * Tests range 0 <= col < 4
     */
    @Test
    public void testRangeQuery3() {
        UUID txId = new UUID(0, 0);

        insert(txId);

        List<VersionChain<Tuple>> rows =
                store.query(new RangeQuery(0, Tuple.create(0), true, Tuple.create(4), false), txId).loadAll(new ArrayList<>()).join();

        validateRows(rows, 2, 0, txId);
    }

    /**
     * Tests range 0 < col < 4
     */
    @Test
    public void testRangeQuery4() {
        UUID txId = new UUID(0, 0);

        insert(txId);

        List<VersionChain<Tuple>> rows =
                store.query(new RangeQuery(0, Tuple.create(0), false, Tuple.create(4), false), txId).loadAll(new ArrayList<>()).join();

        validateRows(rows, 1, 2, txId);
    }

    /**
     * Tests range 2 <= col
     */
    @Test
    public void testRangeQueryOpenUpper() {
        UUID txId = new UUID(0, 0);

        insert(txId);

        List<VersionChain<Tuple>> rows =
                store.query(new RangeQuery(0, Tuple.create(2), true, null, true), txId).loadAll(new ArrayList<>()).join();

        validateRows(rows, 3, 2, txId);
    }

    /**
     * Tests range 2 < col
     */
    @Test
    public void testRangeQueryOpenUpper2() {
        UUID txId = new UUID(0, 0);

        insert(txId);

        List<VersionChain<Tuple>> rows =
                store.query(new RangeQuery(0, Tuple.create(2), false, null, true), txId).loadAll(new ArrayList<>()).join();

        validateRows(rows, 2, 4, txId);
    }

    /**
     * Tests range col <= 2
     */
    @Test
    public void testRangeQueryOpenLower() {
        UUID txId = new UUID(0, 0);

        insert(txId);

        List<VersionChain<Tuple>> rows =
                store.query(new RangeQuery(0, null, true, Tuple.create(2), true), txId).loadAll(new ArrayList<>()).join();

        validateRows(rows, 2, 0, txId);
    }

    /**
     * Tests range col < 2
     */
    @Test
    public void testRangeQueryOpenLower2() {
        UUID txId = new UUID(0, 0);

        insert(txId);

        List<VersionChain<Tuple>> rows =
                store.query(new RangeQuery(0, null, true, Tuple.create(2), false), txId).loadAll(new ArrayList<>()).join();

        validateRows(rows, 1, 0, txId);
    }

    @Test
    public void testInsertGet_3TX() {
        UUID txId = new UUID(0, 0);
        UUID txId2 = new UUID(0, 1);
        UUID txId3 = new UUID(0, 2);

        store.insert(Tuple.create(0, "val0"), txId).join();
        store.insert(Tuple.create(1, "val1"), txId).join();
        store.insert(Tuple.create(2, "val2"), txId).join();

        store.insert(Tuple.create(3, "val3"), txId2).join();

        CompletableFuture<List<VersionChain<Tuple>>> fut = store
                .query(new RangeQuery(0, null, true, null, false), txId3).loadAll(new ArrayList<>());

        assertFalse(fut.isDone());
        store.commit(txId, Timestamp.now());
        assertFalse(fut.isDone());
        store.commit(txId2, Timestamp.now());

        List<VersionChain<Tuple>> rows = fut.join();
        assertEquals(4, rows.size());
    }

    @Test
    public void testInsertGetAbort_3TX() {
        UUID txId = new UUID(0, 0);
        UUID txId2 = new UUID(0, 1);
        UUID txId3 = new UUID(0, 2);

        // Insert tx1
        store.insert(Tuple.create(0, "val0"), txId).join();
        store.insert(Tuple.create(1, "val1"), txId).join();
        store.insert(Tuple.create(2, "val2"), txId).join();

        // Insert tx2
        store.insert(Tuple.create(3, "val3"), txId2).join();

        // Scan tx3
        CompletableFuture<List<VersionChain<Tuple>>> fut = store
                .query(new RangeQuery(0, null, true, null, false), txId3).loadAll(new ArrayList<>());

        assertFalse(fut.isDone());
        store.abort(txId);
        assertFalse(fut.isDone());
        store.abort(txId2);

        List<VersionChain<Tuple>> rows = fut.join();
        assertTrue(rows.isEmpty());
    }

    @Test
    public void testInsertGetEmpty() {
        UUID txId = new UUID(0, 0);

        store.insert(Tuple.create(1, "val1"), txId).join();

        List<VersionChain<Tuple>> rows = store.query(new RangeQuery(0, Tuple.create(0), true, null, true), txId)
                .loadAll(new ArrayList<>()).join();

        assertEquals(1, rows.size());

        var err = assertThrows(CompletionException.class, () -> store.insert(Tuple.create(1, "val2"), txId).join());
        assertEquals(UniqueException.class, err.getCause().getClass());

        store.insert(Tuple.create(2, "val2"), txId).join();

        rows = store.query(new RangeQuery(0, Tuple.create(0), true, null, true), txId).loadAll(new ArrayList<>()).join();

        assertEquals(2, rows.size());
    }

    private void validateRows(List<VersionChain<Tuple>> rows, int expCnt, int idx, UUID txId) {
        assertEquals(expCnt, rows.size());

        for (int i = 0; i < rows.size(); i++) {
            int i1 = idx + i * 2;
            assertEquals(Tuple.create(i1, "val" + i1), store.get(rows.get(i), txId, null).join());
            assertEquals(Tuple.create(i1, "val" + i1), getSingle(txId, 0, Tuple.create(i1)));
        }
    }

    private void insert(UUID txId) {
        store.insert(Tuple.create(0, "val0"), txId).join();
        store.insert(Tuple.create(2, "val2"), txId).join();
        store.insert(Tuple.create(4, "val4"), txId).join();
        store.insert(Tuple.create(6, "val6"), txId).join();
    }
}
