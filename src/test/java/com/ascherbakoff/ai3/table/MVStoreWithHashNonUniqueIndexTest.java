package com.ascherbakoff.ai3.table;

import com.ascherbakoff.ai3.lock.DeadlockPrevention;
import com.ascherbakoff.ai3.lock.LockTable;
import java.util.Map;

public class MVStoreWithHashNonUniqueIndexTest extends MVStoreNonUniqueIndexBasicTest {
    public MVStoreWithHashNonUniqueIndexTest() {
        {
            VersionChainRowStore<Tuple> rowStore = new VersionChainRowStore<>();
            store = new MVStoreImpl(
                    rowStore,
                    new LockTable(10, true, DeadlockPrevention.none()),
                    Map.of(0, new HashNonUniqueIndex(0, new LockTable(10, true, DeadlockPrevention.none()), new HashIndexStoreImpl<>(), rowStore))
            );
        }
    }
}
