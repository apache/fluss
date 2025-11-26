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

package org.apache.fluss.server.log;

import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.record.FileLogProjection;
import org.apache.fluss.record.TestData;
import org.apache.fluss.record.TestingSchemaGetter;

import org.junit.jupiter.api.Test;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for {@link org.apache.fluss.server.log.FetchParams}. */
class FetchParamsTest {

    @Test
    void testSetCurrentFetch() {
        FetchParams fetchParams = new FetchParams(1, 100);
        fetchParams.setCurrentFetch(
                1L,
                20L,
                1024,
                new TestingSchemaGetter(new SchemaInfo(TestData.DATA1_SCHEMA, (short) 1)),
                DEFAULT_COMPRESSION,
                null);
        assertThat(fetchParams.fetchOffset()).isEqualTo(20L);
        assertThat(fetchParams.maxFetchBytes()).isEqualTo(1024);
        assertThat(fetchParams.projection()).isNull();

        fetchParams.setCurrentFetch(
                2L,
                30L,
                512,
                new TestingSchemaGetter(new SchemaInfo(TestData.DATA2_SCHEMA, (short) 1)),
                DEFAULT_COMPRESSION,
                new int[] {0, 2});
        assertThat(fetchParams.fetchOffset()).isEqualTo(30L);
        assertThat(fetchParams.maxFetchBytes()).isEqualTo(512);
        assertThat(fetchParams.projection()).isNotNull();

        FileLogProjection prevProjection = fetchParams.projection();

        fetchParams.setCurrentFetch(
                1L,
                40L,
                256,
                new TestingSchemaGetter(new SchemaInfo(TestData.DATA1_SCHEMA, (short) 1)),
                DEFAULT_COMPRESSION,
                null);
        assertThat(fetchParams.projection()).isNull();

        fetchParams.setCurrentFetch(
                2L,
                30L,
                512,
                new TestingSchemaGetter(new SchemaInfo(TestData.DATA2_SCHEMA, (short) 1)),
                DEFAULT_COMPRESSION,
                new int[] {0, 2});
        // the FileLogProjection should be cached
        assertThat(fetchParams.projection()).isNotNull().isSameAs(prevProjection);
    }
}
