package com.ascherbakoff.ai3.table;

import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.lock.DeadlockPrevention;
import com.ascherbakoff.ai3.lock.LockTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Disabled;

public class MVStoreFullScanTest {
    MVStoreImpl store;

    {
        VersionChainRowStore<Tuple> rowStore = new VersionChainRowStore<>();
        store = new MVStoreImpl(
                rowStore,
                new LockTable(10, true, DeadlockPrevention.none()),
                Map.of()
        );
    }

    /**
     * TODO FIXME this tests demonstrates broken snapshot isolation for scan queries. It can be fixed by using table locks, which are
     * not yet implemented.
     */
    @Disabled
    public void testConcurrentTransactions() {
        UUID txId1 = new UUID(0, 0);
        store.insert(Tuple.create(0, "val0"), txId1).join();

        UUID txId2 = new UUID(0, 1);
        store.insert(Tuple.create(1, "val1"), txId2).join();

        CompletableFuture<List<VersionChain<Tuple>>> fut1 = store.query(new ScanQuery(), txId1).loadAll(new ArrayList<>());

        assertFalse(supplyAsync(() -> fut1.isDone(), delayedExecutor(200, TimeUnit.MILLISECONDS)).join());

        store.commit(txId2, Timestamp.now());

        List<VersionChain<Tuple>> rows = fut1.join();

        assertEquals(2, rows.size());
    }
}
