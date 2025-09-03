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

package org.apache.fluss.lake.writer;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

/**
 * The WriterInitContext interface provides the context needed to create a LakeWriter. It includes
 * methods to obtain the table path, table bucket, an optional partition, and table metadata
 * information.
 *
 * @since 0.7
 */
@PublicEvolving
public interface WriterInitContext {

    /**
     * Returns the table path.
     *
     * @return the table path
     */
    TablePath tablePath();

    /**
     * Returns the table bucket.
     *
     * @return the table bucket
     */
    TableBucket tableBucket();

    /**
     * Returns the partition, or null if there is no partition.
     *
     * @return the partition, or null if there is no partition
     */
    @Nullable
    String partition();

    /**
     * Returns the Fluss table info.
     *
     * @return the Fluss table info
     */
    TableInfo tableInfo();
}
