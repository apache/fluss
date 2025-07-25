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

package com.alibaba.fluss.predicate;

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.types.DataType;

import static java.lang.Math.min;

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Utils for comparator. */
public class CompareUtils {
    private CompareUtils() {}

    public static int compareLiteral(DataType type, Object v1, Object v2) {
        if (v1 instanceof Comparable) {
            // because BinaryString can not serialize so v1 or v2 may be BinaryString convert to
            // String for compare
            if (v1 instanceof BinaryString) {
                v1 = ((BinaryString) v1).toString();
            }
            if (v2 instanceof BinaryString) {
                v2 = ((BinaryString) v2).toString();
            }
            return ((Comparable<Object>) v1).compareTo(v2);
        } else if (v1 instanceof byte[]) {
            return compare((byte[]) v1, (byte[]) v2);
        } else {
            throw new RuntimeException("Unsupported type: " + type);
        }
    }

    private static int compare(byte[] first, byte[] second) {
        for (int x = 0; x < min(first.length, second.length); x++) {
            int cmp = first[x] - second[x];
            if (cmp != 0) {
                return cmp;
            }
        }
        return first.length - second.length;
    }
}
