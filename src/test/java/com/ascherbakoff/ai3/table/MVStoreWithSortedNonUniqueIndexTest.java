package com.ascherbakoff.ai3.table;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.lock.DeadlockPrevention;
import com.ascherbakoff.ai3.lock.LockTable;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class MVStoreWithSortedNonUniqueIndexTest extends MVStoreNonUniqueIndexBasicTest {
    public MVStoreWithSortedNonUniqueIndexTest() {
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
}
