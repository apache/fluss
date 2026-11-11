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

package org.apache.fluss.rpc.protocol;

import org.apache.fluss.rpc.messages.PbUnionReadContext;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufUtil;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

/** Shared Java/Rust fixture for the UnionRead boundary wire contract. */
class UnionReadBoundaryTest {
    @Test
    void testSharedBoundaryFixture() throws Exception {
        String hex =
                Files.readAllLines(
                                Paths.get(
                                        "../fluss-rust/crates/fluss-lake/tests/fixtures/union-read-v1.txt"))
                        .stream()
                        .filter(line -> !line.startsWith("#") && !line.isEmpty())
                        .findFirst()
                        .get();
        ByteBuf buffer = Unpooled.buffer();
        try {
            boundary().writeTo(buffer);
            assertThat(ByteBufUtil.hexDump(buffer)).isEqualTo(hex);
        } finally {
            buffer.release();
        }
    }

    static PbUnionReadContext boundary() {
        PbUnionReadContext context =
                new PbUnionReadContext()
                        .setVersion(1)
                        .setDatabaseName("fluss")
                        .setTableName("orders")
                        .setTableId(7)
                        .setSchemaId(2)
                        .setTableBucketCount(2)
                        .setCoverage(1);
        context.addBucketKey("id");
        context.addPartitionKey("region");
        context.setLake()
                .setFormat("paimon")
                .setDatabaseName("fluss")
                .setTableName("orders")
                .setSnapshotId(42);
        context.setPartitionScope()
                .setType(0)
                .setLeaf()
                .setFunction(5)
                .setFieldId(1)
                .addLiteral()
                .setLiteralType(8)
                .setIsNull(false)
                .setStringValue("US");
        context.addLogRange()
                .setPartitionId(9)
                .setBucketId(1)
                .setBucketCount(2)
                .setBaseline(1)
                .setStartOffset(0)
                .setStopOffset(20)
                .setEarliestOffset(0)
                .addPartitionValue("US");
        return context;
    }
}
