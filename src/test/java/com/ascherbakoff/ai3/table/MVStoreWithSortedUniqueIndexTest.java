package com.ascherbakoff.ai3.table;

import com.ascherbakoff.ai3.lock.DeadlockPrevention;
import com.ascherbakoff.ai3.lock.LockTable;
import java.util.Map;

public class MVStoreWithSortedUniqueIndexTest extends MVStoreWithUniqueIndexBasicTest {
    public MVStoreWithSortedUniqueIndexTest() {
        {
            VersionChainRowStore<Tuple> rowStore = new VersionChainRowStore<>();
            store = new MVStoreImpl(
                    rowStore,
                    new LockTable(10, true, DeadlockPrevention.none()),
                    Map.of(0, new SortedUniqueIndex(0, new LockTable(10, true, DeadlockPrevention.none()), new SortedIndexStoreImpl<>(), rowStore))
            );
        }
    }
}
