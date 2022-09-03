package com.ascherbakoff.ai3.table;

import java.util.ArrayList;
import java.util.List;

public interface Cursor<T> {
    T next();

    default List<T> getAll() {
        List<T> values = new ArrayList<>();

        while(true) {
            T next = next();

            if (next == null)
                break;

            values.add(next);
        }

        return values;
    }
}
