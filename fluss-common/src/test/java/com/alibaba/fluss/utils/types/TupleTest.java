/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.utils.types;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link com.alibaba.fluss.utils.types.Tuple}. */
public class TupleTest {

    @Test
    void testTuple2() {
        // test equal
        Tuple2<Integer, String> t1 = Tuple2.of(1, "2");
        Tuple2<Integer, String> t2 = Tuple2.of(1, "3");
        Tuple2<Integer, String> t4 = Tuple2.of(1, "2");
        assertThat(t1).isNotEqualTo(t2);
        assertThat(t1).isEqualTo(t4);

        // test copy
        Tuple2<Integer, String> t1Copy = t1.copy();
        assertThat(t1Copy.f0).isEqualTo(1);
        assertThat(t1Copy.f1).isEqualTo("2");

        // test to string
        assertThat(t1.toString()).isEqualTo("(1,2)");

        // test tuple2 as an element of Set
        Set<Tuple2<Integer, String>> tuple2Set = new HashSet<>();
        tuple2Set.add(t1);
        assertThat(tuple2Set).contains(Tuple2.of(1, "2"));
    }
}
