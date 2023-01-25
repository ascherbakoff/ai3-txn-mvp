package com.ascherbakoff.ai3.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.ascherbakoff.ai3.lock.DeadlockPrevention;
import com.ascherbakoff.ai3.lock.LockException;
import com.ascherbakoff.ai3.lock.LockTable;
import com.ascherbakoff.ai3.util.BasicTest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

public class MVStoreDeadlockTest extends BasicTest {
    MVStoreImpl store;

    public MVStoreDeadlockTest() {
        {
            VersionChainRowStore<Tuple> rowStore = new VersionChainRowStore<>();
            store = new MVStoreImpl(
                    rowStore,
                    new LockTable(10, true, DeadlockPrevention.waitDie()),
                    Map.of(0, new HashUniqueIndex(0, new LockTable(10, true, DeadlockPrevention.waitDie()), new HashIndexStoreImpl<>(), rowStore))
            );
        }
    }

    @Test
    public void testWrongOrder() {
        UUID id1 = new UUID(0, 0);
        UUID id2 = new UUID(0, 1);
        UUID id3 = new UUID(0, 2);

        VersionChain<Tuple> rowId = store.insert(Tuple.create(0, "val0"), id1).join();
        store.commit(id1, clock.tick());

        VersionChain<Tuple> row0 = store.query(new EqQuery(0, Tuple.create(0)), id2).loadAll(new ArrayList<>()).join().get(0);
        VersionChain<Tuple> row1 = store.query(new EqQuery(0, Tuple.create(0)), id3).loadAll(new ArrayList<>()).join().get(0);

        assertSame(row0, row1);

        Tuple tup0 = store.get(row0, id2, null).join();
        assertEquals(tup0, Tuple.create(0, "val0"));

        Tuple tup1 = store.get(row0, id3, null).join();
        assertEquals(tup1, Tuple.create(0, "val0"));

        assertThrows(LockException.class, () -> store.update(row0, Tuple.create(0, "val1"), id3));
    }

    /**
     * TODO FIXME reproduces deadlock on table locks, which are not yet implemented.
     */
    @Test
    public void testConcurrentTransactions() {
        UUID txId1 = new UUID(0, 0);
        store.insert(Tuple.create(0, "val0"), txId1).join();

        UUID txId2 = new UUID(0, 1);
        store.insert(Tuple.create(1, "val1"), txId2).join();

        CompletableFuture<List<VersionChain<Tuple>>> fut1 = store.query(new ScanQuery(), txId1).loadAll(new ArrayList<>());
        assertFalse(fut1.isDone());

        CompletableFuture<List<VersionChain<Tuple>>> fut2 = store.query(new ScanQuery(), txId2).loadAll(new ArrayList<>());
        assertFalse(fut2.isDone());
    }
}
