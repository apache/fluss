/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.server.kv.autoinc;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TablePath;

/** Factory interface for creating {@link SequenceGenerator} instances. */
public interface SequenceGeneratorFactory {

    /**
     * Creates a {@link SequenceGenerator} for the specified table and auto-increment column.
     *
     * @param tablePath the path of the table
     * @param autoIncrementColumn the auto-increment column schema
     * @param idCacheSize the size of ID cache for optimization
     * @return a new instance of {@link SequenceGenerator}
     */
    SequenceGenerator createSequenceGenerator(
            TablePath tablePath, Schema.Column autoIncrementColumn, long idCacheSize);
}
