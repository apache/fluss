/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.codegen.types;

import org.apache.fluss.row.InternalRow;

import java.io.Serializable;

/**
 * Record equaliser for InternalRow which can compare two InternalRow instances and returns whether
 * they are equal.
 *
 * <p>This interface is implemented by generated classes at runtime for high-performance record
 * comparison without boxing overhead.
 */
public interface RecordEqualiser extends Serializable {

    /**
     * Returns {@code true} if the rows are equal to each other and {@code false} otherwise.
     *
     * @param row1 the first row to compare
     * @param row2 the second row to compare
     * @return true if the rows are equal, false otherwise
     */
    boolean equals(InternalRow row1, InternalRow row2);
}
