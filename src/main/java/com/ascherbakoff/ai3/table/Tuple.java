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

/**
 *
 */
public interface Tuple extends Comparable<Tuple> {
    static Tuple TOMBSTONE = Tuple.create();
    static Tuple INF = Tuple.create();

    <T> T get(int index);

    void set(int index, Comparable value);

    int length();

    Tuple select(int... indexes);

    public static Tuple create(Comparable... values) {
        return new TupleImpl(values);
    }
}
