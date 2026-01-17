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

import java.util.concurrent.atomic.AtomicLong;

/** Factory for creating {@link SequenceGenerator} instances for testing. */
public class TestingSequenceGeneratorFactory implements SequenceGeneratorFactory {

    @Override
    public SequenceGenerator createSequenceGenerator(
            TablePath tablePath, Schema.Column autoIncrementColumn, long idCacheSize) {
        return new BoundedSegmentSequenceGenerator(
                tablePath,
                autoIncrementColumn.getName(),
                new TestingSequenceIDCounter(new AtomicLong(0)),
                idCacheSize,
                Long.MAX_VALUE);
    }
}
