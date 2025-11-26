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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.data.ZkData.PartitionZNode;
import org.apache.fluss.server.zk.data.ZkData.SchemaZNode;
import org.apache.fluss.server.zk.data.ZkData.TableZNode;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link org.apache.fluss.server.zk.data.ZkData}. */
public class ZkDataTest {

    @Test
    void testParseTablePath() {
        String path = "/metadata/databases/db1/tables/t1";
        TablePath tablePath = TableZNode.parsePath(path);
        assertThat(tablePath).isNotNull().isEqualTo(TablePath.of("db1", "t1"));

        // invalid path
        assertThat(TableZNode.parsePath("/metadata/databases/db1/tables/t1/")).isNull();
        assertThat(TableZNode.parsePath("/metadata/databases/db1/t1/")).isNull();
        assertThat(TableZNode.parsePath("/metadata/databases/db1/tables")).isNull();
        assertThat(TableZNode.parsePath("/metadata/databases/db1/tables/t1/buckets")).isNull();
        assertThat(TableZNode.parsePath("/tabletservers/db1/tables/t1")).isNull();
        assertThat(TableZNode.parsePath(path + "/partitions/20240911")).isNull();
    }

    @Test
    void testParsePartitionPath() {
        String path = "/metadata/databases/db1/tables/t1/partitions/20240911";
        PhysicalTablePath tablePath = PartitionZNode.parsePath(path);
        assertThat(tablePath).isNotNull().isEqualTo(PhysicalTablePath.of("db1", "t1", "20240911"));
        assertThat(tablePath.toString()).isEqualTo("db1.t1(p=20240911)");

        // invalid path
        assertThat(TableZNode.parsePath(path + "/")).isNull();
        assertThat(TableZNode.parsePath(path + "/buckets")).isNull();
        assertThat(TableZNode.parsePath(path + "*")).isNull();
        assertThat(TableZNode.parsePath("/metadata/databases/db1/t1/20240911")).isNull();
        assertThat(TableZNode.parsePath("/metadata/databases/db1/tables/t1/partitions")).isNull();
        assertThat(TableZNode.parsePath("/metadata/databases/db1/tables/t1/partitions/")).isNull();
        assertThat(TableZNode.parsePath("/metadata/databases/db1/tables/*t1*/partitions/20240911"))
                .isNull();
    }

    @Test
    void testParseSchemaId() {
        String path = "/metadata/databases/db1/tables/t1/schemas/1";
        Tuple2<TablePath, Integer> tablePathAndSchemaId = SchemaZNode.parsePath(path);
        assertThat(tablePathAndSchemaId)
                .isNotNull()
                .isEqualTo(Tuple2.of(TablePath.of("db1", "t1"), 1));

        // invalid path
        assertThat(SchemaZNode.parsePath(path + "/")).isNull();
        assertThat(SchemaZNode.parsePath(path + "/buckets")).isNull();
        assertThat(SchemaZNode.parsePath(path + "*")).isNull();
        assertThat(SchemaZNode.parsePath("/metadata/databases/db1/t1/20240911")).isNull();
        assertThat(SchemaZNode.parsePath("/metadata/databases/db1/tables/t1/schemas/a")).isNull();
    }
}
