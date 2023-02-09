package com.ascherbakoff.ai3.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ascherbakoff.ai3.clock.Clock;
import com.ascherbakoff.ai3.clock.ManualTimeProvider;
import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.util.BasicTest;
import java.util.List;
import java.util.Map.Entry;
import org.junit.jupiter.api.Test;

public class MVKeyValueTableTest extends BasicTest {
    @Test
    public void testBasic() {
        Clock clock = new Clock(new ManualTimeProvider());

        MVKeyValueTable<Integer, String> t = new MVKeyValueTable<>();

        final int key = 0;
        final String val = "test0";

        assertNull(t.get(key, clock.now()));

        Timestamp ts = clock.now();
        assertEquals(null, t.put(key, val, ts));

        assertEquals(val, t.get(key, ts));

        Timestamp ts2 = clock.now();
        assertEquals(val, t.remove(key, ts2));

        assertNull(t.get(key, ts2));
        assertEquals(val, t.get(key, ts));

        final String val2 = "test1";

        Timestamp ts3 = clock.now();
        assertEquals(null, t.put(key, val2, ts3));

        assertEquals(val2, t.get(key, ts3));

        List<Entry<Integer, String>> rows0 = t.scan(ts).getAll();
        assertTrue(rows0.size() == 1 && val.equals(rows0.get(0).getValue()));

        List<Entry<Integer, String>> rows1 = t.scan(ts2).getAll();
        assertTrue(rows1.isEmpty());

        List<Entry<Integer, String>> rows2 = t.scan(ts3).getAll();
        assertTrue(rows2.size() == 1 && val2.equals(rows2.get(0).getValue()));

        System.out.println();
    }
}
