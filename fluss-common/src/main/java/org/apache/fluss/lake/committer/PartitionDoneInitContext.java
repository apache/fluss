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

package org.apache.fluss.lake.committer;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

/**
 * The context needed to create a {@link PartitionDoneHandler}.
 *
 * <p>It provides the table path, the table info (which carries partition keys, table config
 * including the {@code table.datalake.partition.*} mark-done options, and auto-partition strategy)
 * and the Fluss client configuration. Lake implementations read the mark-done related options from
 * {@link TableInfo#getTableConfig()}.
 *
 * @since 0.9
 */
@PublicEvolving
public interface PartitionDoneInitContext {

    /**
     * Returns the table path.
     *
     * @return the table path
     */
    TablePath tablePath();

    /**
     * Returns the table info, which carries partition keys, table config (including the {@code
     * table.datalake.partition.*} mark-done options) and auto-partition strategy.
     *
     * @return the table info
     */
    TableInfo tableInfo();

    /**
     * Returns the Fluss client configuration. This configuration can be used to build a Fluss
     * client, such as a Connection.
     *
     * @return the Fluss client configuration
     */
    Configuration flussClientConfig();
}
