package com.ascherbakoff.ai3.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.ascherbakoff.ai3.util.BasicTest;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class KvTableTest extends BasicTest {
    @Test
    public void testBasic() {
        KvTable<Integer, String> t = new KvTable<>();

        UUID tx1 = new UUID(0, 0);

        final int key = 0;
        final String val = "test0";

        assertNull(t.get(key, tx1).join());

        Tuple tup = Tuple.create(key, val);
        assertEquals(key, t.insert(tup, tx1).join());

        assertEquals(val, t.get(key, tx1).join());

        assertEquals(tup, t.remove(key, tx1).join());

        assertNull(t.get(key, tx1).join());
    }
}
