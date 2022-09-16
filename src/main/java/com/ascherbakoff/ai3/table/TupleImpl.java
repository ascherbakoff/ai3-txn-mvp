/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ascherbakoff.ai3.table;

import java.util.Arrays;
import org.jetbrains.annotations.NotNull;

/**
 * Simple tuple implementation.
 */
class TupleImpl implements Tuple {
    private final Comparable[] values;

    TupleImpl(Comparable... values) {
        this.values = values;
    }

    @Override
    public <T> T get(int index) {
        return (T) values[index];
    }

    @Override
    public void set(int index, Comparable value) {
        values[index] = value;
    }

    @Override
    public int length() {
        return values.length;
    }

    @Override
    public Tuple select(int... indexes) {
        Comparable[] tmp = new Comparable[indexes.length];

        for (int i = 0; i < indexes.length; i++) {
            tmp[i] = values[indexes[i]];
        }

        return Tuple.create(tmp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TupleImpl tuple = (TupleImpl) o;

        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(values, tuple.values)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    @Override
    public int compareTo(@NotNull Tuple o) {
        TupleImpl tuple = (TupleImpl) o;

        assert length() == tuple.length();

        return Arrays.compare(values, tuple.values);
    }

    @Override
    public String toString() {
        return "TupleImpl{" +
                "values=" + Arrays.toString(values) +
                '}';
    }
}
