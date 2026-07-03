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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link WriterInitContext}. */
class WriterInitContextTest {

    @Test
    void testDefaultTieringRoundMetadata() {
        WriterInitContext context =
                new WriterInitContext() {
                    @Override
                    public TablePath tablePath() {
                        return null;
                    }

                    @Override
                    public TableBucket tableBucket() {
                        return null;
                    }

                    @Nullable
                    @Override
                    public String partition() {
                        return null;
                    }

                    @Override
                    public TableInfo tableInfo() {
                        return null;
                    }
                };

        assertThat(context.splitIndex()).isEqualTo(WriterInitContext.UNKNOWN_SPLIT_INDEX);
        assertThat(context.tieringRoundTimestamp())
                .isEqualTo(WriterInitContext.UNKNOWN_TIERING_ROUND_TIMESTAMP);
        assertThat(context.ioTmpDir()).isNull();
    }
}
