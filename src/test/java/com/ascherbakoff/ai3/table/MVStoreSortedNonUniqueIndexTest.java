package com.ascherbakoff.ai3.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.lock.DeadlockPrevention;
import com.ascherbakoff.ai3.lock.LockTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class MVStoreSortedNonUniqueIndexTest extends MVStoreBasicNonUniqueIndexTest {
    public MVStoreSortedNonUniqueIndexTest() {
        {
            VersionChainRowStore<Tuple> rowStore = new VersionChainRowStore<>();
            store = new MVStoreImpl(
                    rowStore,
                    new LockTable(10, true, DeadlockPrevention.none()),
                    Map.of(0, new SortedNonUniqueIndex(0, new LockTable(10, true, DeadlockPrevention.none()), new SortedIndexStoreImpl<>(), rowStore))
            );
        }
    }

    @Test
    public void testInsertGetRange() {
        UUID txId = new UUID(0, 0);

        store.insert(Tuple.create(0, "val0"), txId).join();
        store.insert(Tuple.create(0, "val1"), txId).join();
        store.insert(Tuple.create(0, "val2"), txId).join();
        store.insert(Tuple.create(1, "val1"), txId).join();
        store.insert(Tuple.create(1, "val2"), txId).join();
        store.insert(Tuple.create(2, "val2"), txId).join();
        store.commit(txId, Timestamp.now());

        assertEquals(3, store.query(new RangeQuery(0, Tuple.create(0), true, Tuple.create(0), true), txId)
                .loadAll(new ArrayList<>()).join().size());

        assertEquals(2, store.query(new RangeQuery(0, Tuple.create(1), true, Tuple.create(1), true), txId)
                .loadAll(new ArrayList<>()).join().size());

        assertEquals(1, store.query(new RangeQuery(0, Tuple.create(2), true, Tuple.create(2), true), txId)
                .loadAll(new ArrayList<>()).join().size());
    }

    @Test
    public void testInsertGetEmpty() {
        UUID txId = new UUID(0, 0);

        store.insert(Tuple.create(1, "val1"), txId).join();

        List<VersionChain<Tuple>> rows = store.query(new RangeQuery(0, Tuple.create(0), true, null, true), txId)
                .loadAll(new ArrayList<>()).join();

        assertEquals(1, rows.size());

        store.insert(Tuple.create(1, "val2"), txId).join();
        store.insert(Tuple.create(2, "val3"), txId).join();

        rows = store.query(new RangeQuery(0, Tuple.create(0), true, null, true), txId)
                .loadAll(new ArrayList<>()).join();

        assertEquals(3, rows.size());
    }

    @Test
    public void testInsertGet_3TX() {
        UUID txId = new UUID(0, 0);
        UUID txId2 = new UUID(0, 1);
        UUID txId3 = new UUID(0, 2);

        store.insert(Tuple.create(0, "val0"), txId).join();
        store.insert(Tuple.create(0, "val1"), txId).join();
        store.insert(Tuple.create(0, "val2"), txId).join();

        store.insert(Tuple.create(1, "val3"), txId2).join();
        store.insert(Tuple.create(1, "val4"), txId2).join();

        CompletableFuture<List<VersionChain<Tuple>>> fut = store
                .query(new RangeQuery(0, null, true, null, false), txId3).loadAll(new ArrayList<>());

        assertFalse(fut.isDone());
        store.commit(txId, Timestamp.now());
        assertFalse(fut.isDone());
        store.commit(txId2, Timestamp.now());

        List<VersionChain<Tuple>> rows = fut.join();
        assertEquals(5, rows.size());
    }

    @Test
    public void testInsertGetAbort_3TX() {
        UUID txId = new UUID(0, 0);
        UUID txId2 = new UUID(0, 1);
        UUID txId3 = new UUID(0, 2);

        // Insert tx1
        store.insert(Tuple.create(0, "val0"), txId).join();
        store.insert(Tuple.create(0, "val1"), txId).join();
        store.insert(Tuple.create(0, "val2"), txId).join();

        // Insert tx2
        store.insert(Tuple.create(1, "val3"), txId2).join();
        store.insert(Tuple.create(1, "val4"), txId2).join();

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

    /**
     * Let us assume that T1 had done a range scan from 2 through 10.
     * If now T1 were to insert 8 and it were to lock 8 only in the IX mode, then that would permit T2 to insert 7 (T2’s request of the IX
     * lock on 7 would be compatible with the IX lock held by T1) and commit. If now the T1 were to repeat its scan, then it would
     * retrieve 7, which would be a violation of the serializability. When T1 requested IX on 10, during the insert of 8, and found that
     * it already had an S lock on 10, then it should have obtained an X lock on 8. The latter would have prevented T2 from inserting
     * 7 until Tl committed.
     */
    @Test
    public void testLockStateReplication() {
        UUID txId = new UUID(0, 1);
        UUID txId2 = new UUID(0, 2);

        store.insert(Tuple.create(1, "val0"), txId).join();
        store.insert(Tuple.create(2, "val2"), txId).join();
        store.insert(Tuple.create(5, "val5"), txId).join();
        store.insert(Tuple.create(10, "val10"), txId).join();
        store.insert(Tuple.create(20, "val20"), txId).join();

        List<VersionChain<Tuple>> rows = store.query(new RangeQuery(0, Tuple.create(2), true, Tuple.create(10), true), txId)
                .loadAll(new ArrayList<>()).join();

        assertEquals(3, rows.size());

        store.insert(Tuple.create(8, "val8"), txId).join();

        CompletableFuture<VersionChain<Tuple>> fut = store.insert(Tuple.create(7, "val7"), txId2);
        assertFalse(fut.isDone());

        rows = store.query(new RangeQuery(0, Tuple.create(2), true, Tuple.create(10), true), txId)
                .loadAll(new ArrayList<>()).join();

        assertEquals(4, rows.size());

        store.commit(txId, Timestamp.now());

        fut.join();

        rows = store.query(new RangeQuery(0, Tuple.create(2), true, Tuple.create(10), true), txId2)
                .loadAll(new ArrayList<>()).join();

        assertEquals(5, rows.size());
    }

    /**
     * T1 might have a SIX lock on 10 because it had first inserted 10 (getting a commit duration IX on it) and later did a scan of 2
     * through 10 (getting a commit duration S lock on 10, which causes the resultant hold mode to be changed from IX to SIX).
     */
    @Test
    public void testLockStateReplication2() {
        UUID txId = new UUID(0, 1);
        UUID txId2 = new UUID(0, 2);

        store.insert(Tuple.create(1, "val0"), txId).join();
        store.insert(Tuple.create(2, "val2"), txId).join();
        store.insert(Tuple.create(5, "val5"), txId).join();
        store.insert(Tuple.create(20, "val20"), txId).join();

        List<VersionChain<Tuple>> rows = store.query(new RangeQuery(0, Tuple.create(2), true, Tuple.create(10), true), txId)
                .loadAll(new ArrayList<>()).join();

        assertEquals(2, rows.size());

        store.insert(Tuple.create(10, "val10"), txId).join();

        rows = store.query(new RangeQuery(0, Tuple.create(2), true, Tuple.create(10), true), txId)
                .loadAll(new ArrayList<>()).join();

        assertEquals(3, rows.size());

        store.insert(Tuple.create(8, "val8"), txId).join();

        CompletableFuture<VersionChain<Tuple>> fut = store.insert(Tuple.create(7, "val7"), txId2);
        assertFalse(fut.isDone());

        rows = store.query(new RangeQuery(0, Tuple.create(2), true, Tuple.create(10), true), txId)
                .loadAll(new ArrayList<>()).join();

        assertEquals(4, rows.size());

        store.commit(txId, Timestamp.now());

        fut.join();

        rows = store.query(new RangeQuery(0, Tuple.create(2), true, Tuple.create(10), true), txId2)
                .loadAll(new ArrayList<>()).join();

        assertEquals(5, rows.size());
    }

    /**
     * T1 might have an X lock on 8 because it had inserted 8 and X state was replicated to 8 due to previously S locked 10.
     * Now, if 7 were to be inserted by T1 and locked only in the IX mode, then that would permit T2 to insert 6 and commit, causing
     * subsequent T1 range scans to see 6.
     */
    @Test
    public void testLockStateReplication3() {
        UUID txId = new UUID(0, 1);
        UUID txId2 = new UUID(0, 2);

        store.insert(Tuple.create(1, "val0"), txId).join();
        store.insert(Tuple.create(2, "val2"), txId).join();
        store.insert(Tuple.create(5, "val5"), txId).join();
        store.insert(Tuple.create(10, "val10"), txId).join();
        store.insert(Tuple.create(20, "val20"), txId).join();

        List<VersionChain<Tuple>> rows = store.query(new RangeQuery(0, Tuple.create(2), true, Tuple.create(10), true), txId)
                .loadAll(new ArrayList<>()).join();

        assertEquals(3, rows.size());

        store.insert(Tuple.create(8, "val8"), txId).join();

        store.insert(Tuple.create(7, "val7"), txId).join();

        CompletableFuture<VersionChain<Tuple>> fut = store.insert(Tuple.create(6, "val6"), txId2);
        assertFalse(fut.isDone());

        rows = store.query(new RangeQuery(0, Tuple.create(2), true, Tuple.create(10), true), txId)
                .loadAll(new ArrayList<>()).join();

        assertEquals(5, rows.size());

        store.commit(txId, Timestamp.now());

        fut.join();

        rows = store.query(new RangeQuery(0, Tuple.create(2), true, Tuple.create(10), true), txId2)
                .loadAll(new ArrayList<>()).join();

        assertEquals(6, rows.size());
    }

    @Test // TODO FIXME !
    public void testLockBetween() throws Exception {
        UUID txId = new UUID(0, 1);

        store.insert(Tuple.create(1, "val0"), txId).join();
        store.insert(Tuple.create(3, "val3"), txId).join();
        store.commit(txId, Timestamp.now());

        UUID txId2 = new UUID(0, 2);

        RangeQuery query = new RangeQuery(0, Tuple.create(1), true, Tuple.create(3), true);
        AsyncCursor<VersionChain<Tuple>> cur = store.query(query, txId2);

        VersionChain<Tuple> tup0 = cur.nextAsync().join();

        query.delayOnNext = new CyclicBarrier(2);

        ExecutorService ex = Executors.newSingleThreadExecutor();
        CompletableFuture<VersionChain<Tuple>> fut = CompletableFuture.supplyAsync(() -> cur.nextAsync(), ex).thenCompose(x -> x);

        assertTrue(waitForCondition(() -> query.delayOnNext.getNumberWaiting() == 1, 1000));

        UUID txId3 = new UUID(0, 3);
        store.insert(Tuple.create(2, "val2"), txId3).join();
        store.commit(txId3, Timestamp.now());

        query.delayOnNext.await();

        VersionChain<Tuple> tup1 = fut.join();

        query.delayOnNext = null;

        assertNull(cur.nextAsync().join());

        List<VersionChain<Tuple>> rows = store.query(new RangeQuery(0, Tuple.create(1), true, Tuple.create(3), true), txId)
                .loadAll(new ArrayList<>()).join();

        assertEquals(2, rows.size(), "Phantom has appeared");

        ex.shutdown();
        assertTrue(ex.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test
    public void testReenterUnlockSameTx() {
        UUID txId = new UUID(0, 1);
        UUID txId2 = new UUID(0, 2);

        store.insert(Tuple.create(2, "val2"), txId).join();
        store.insert(Tuple.create(1, "val1"), txId).join(); // This insert shouldn't invalidate next lock.

        CompletableFuture<Tuple> fut = getSingleAsync(txId2, 0, Tuple.create(2));
        assertFalse(fut.isDone());

        store.commit(txId, Timestamp.now());
        assertEquals(Tuple.create(2, "val2"), fut.join());
    }

    @Test
    public void testReenterUnlockSameTxRollback() {
        UUID txId = new UUID(0, 1);
        UUID txId2 = new UUID(0, 2);

        store.insert(Tuple.create(2, "val2"), txId).join();
        store.insert(Tuple.create(1, "val1"), txId).join(); // This insert shouldn't invalidate next lock.

        CompletableFuture<Tuple> fut = getSingleAsync(txId2, 0, Tuple.create(2));
        assertFalse(fut.isDone());

        store.abort(txId);
        assertNull(fut.join());
    }
}
