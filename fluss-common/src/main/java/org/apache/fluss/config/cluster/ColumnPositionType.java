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

package org.apache.fluss.config.cluster;

import org.apache.fluss.metadata.TableChange;

/** ColumnPositionType. */
public enum ColumnPositionType {
    LAST(0);

    public final int value;

    ColumnPositionType(int value) {
        this.value = value;
    }

    public static ColumnPositionType from(int opType) {
        if (opType == 0) {
            return LAST;
        }
        throw new IllegalArgumentException("Unsupported ColumnPositionType: " + opType);
    }

    public static ColumnPositionType from(TableChange.ColumnPosition opType) {
        if (opType == TableChange.ColumnPosition.last()) {
            return LAST;
        }
        throw new IllegalArgumentException("Unsupported ColumnPositionType: " + opType);
    }

    public int value() {
        return this.value;
    }
}
