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

package org.apache.fluss.record;

import org.apache.fluss.row.BinaryRow;

/**
 * A value record is a tuple consisting of a value row and a schema id for the row.
 *
 * @since 0.3
 */
public interface ValueRecord {

    /**
     * Get the schema id of the value record.
     *
     * @return the schema id of the value record.
     */
    short schemaId();

    /** Get the value row of the record. */
    BinaryRow getRow();

    /**
     * Get the size in bytes of this record.
     *
     * @return the size of the record in bytes
     */
    int getSizeInBytes();
}
