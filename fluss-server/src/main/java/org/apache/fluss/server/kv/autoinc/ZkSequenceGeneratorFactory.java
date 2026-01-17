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
import org.apache.fluss.server.zk.ZkSequenceIDCounter;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.types.DataTypeRoot;

/** ZooKeeper-based implementation of {@link SequenceGeneratorFactory}. */
public class ZkSequenceGeneratorFactory implements SequenceGeneratorFactory {

    private final ZooKeeperClient zkClient;

    public ZkSequenceGeneratorFactory(ZooKeeperClient zkClient) {
        this.zkClient = zkClient;
    }

    @Override
    public SequenceGenerator createSequenceGenerator(
            TablePath tablePath, Schema.Column autoIncrementColumn, long idCacheSize) {
        DataTypeRoot typeRoot = autoIncrementColumn.getDataType().getTypeRoot();
        final long maxAllowedValue;
        if (typeRoot == DataTypeRoot.INTEGER) {
            maxAllowedValue = Integer.MAX_VALUE;
        } else if (typeRoot == DataTypeRoot.BIGINT) {
            maxAllowedValue = Long.MAX_VALUE;
        } else {
            throw new IllegalArgumentException(
                    "Auto-increment column must be of type INTEGER or BIGINT");
        }
        return new BoundedSegmentSequenceGenerator(
                tablePath,
                autoIncrementColumn.getName(),
                new ZkSequenceIDCounter(
                        zkClient.getCuratorClient(),
                        ZkData.AutoIncrementColumnZNode.path(
                                tablePath, autoIncrementColumn.getColumnId())),
                idCacheSize,
                maxAllowedValue);
    }
}
