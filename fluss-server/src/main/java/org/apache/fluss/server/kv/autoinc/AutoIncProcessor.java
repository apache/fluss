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

package org.apache.fluss.server.kv.autoinc;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.server.zk.ZkSequenceIDCounter;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.ZkData;

/** AutoIncProcessor is used to process auto increment column. */
public interface AutoIncProcessor {

    /**
     * Process auto increment column.
     *
     * @param originalValue the original value.
     * @return the processed value.
     */
    BinaryValue processAutoInc(BinaryValue originalValue);

    static AutoIncProcessor create(
            TablePath tablePath,
            int schemaId,
            Configuration properties,
            TableConfig tableConf,
            SchemaGetter schemaGetter,
            ZooKeeperClient zkClient) {
        // TODO: now we only support one auto increment column. we should support add new auto
        // increment column later.
        Schema schema = schemaGetter.getSchema(schemaId);
        int[] autoIncColumnIds = schema.getAutoIncColumnIds();
        if (autoIncColumnIds.length > 0) {
            IncIDGenerator[] incIDGenerators = new IncIDGenerator[autoIncColumnIds.length];
            for (int i = 0; i < autoIncColumnIds.length; i++) {
                ZkSequenceIDCounter zkSequenceIDCounter =
                        new ZkSequenceIDCounter(
                                zkClient.getCuratorClient(),
                                ZkData.AutoIncrementColumnZNode.path(
                                        tablePath, autoIncColumnIds[i]));
                incIDGenerators[i] =
                        new SegmentIncIDGenerator(
                                tablePath,
                                autoIncColumnIds[i],
                                schema.getColumnName(autoIncColumnIds[i]),
                                zkSequenceIDCounter,
                                properties);
            }
            return new AutoIncColumnProcessor(
                    tableConf.getKvFormat(),
                    (short) schemaId,
                    schema,
                    autoIncColumnIds,
                    incIDGenerators);
        } else {
            return new DefaultProcessor();
        }
    }

    /** Default processor is used when auto increment column is not enabled. */
    class DefaultProcessor implements AutoIncProcessor {
        @Override
        public BinaryValue processAutoInc(BinaryValue originalRow) {
            return originalRow;
        }
    }
}
