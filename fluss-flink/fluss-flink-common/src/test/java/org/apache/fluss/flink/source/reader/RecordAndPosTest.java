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

package org.apache.fluss.flink.source.reader;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.GenericRow;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RecordAndPos}. */
class RecordAndPosTest {

    @Test
    void testRecordAndPos() {
        GenericRow genericRow = new GenericRow(3);
        genericRow.setField(0, 1);
        genericRow.setField(1, null);
        genericRow.setField(2, 3);
        ScanRecord scanRecord = new ScanRecord(0, -1, ChangeType.APPEND_ONLY, genericRow);
        RecordAndPos recordAndPos = new RecordAndPos(scanRecord);
        assertThat(recordAndPos.readRecordsCount()).isEqualTo(-1);
        assertThat(recordAndPos.record()).isEqualTo(scanRecord);

        RecordAndPos recordAndPos1 = new RecordAndPos(scanRecord, -1);
        assertThat(recordAndPos1.readRecordsCount()).isEqualTo(-1);
        assertThat(recordAndPos1).isEqualTo(recordAndPos);
        assertThat(recordAndPos1.hashCode()).isEqualTo(recordAndPos.hashCode());

        RecordAndPos recordAndPos2 = new RecordAndPos(scanRecord, 3);
        assertThat(recordAndPos2.readRecordsCount()).isEqualTo(3);
        assertThat(recordAndPos2).isNotEqualTo(recordAndPos);

        assertThat(recordAndPos.toString())
                .isEqualTo("RecordAndPos{scanRecord=+A(1,null,3)@0, readRecordsCount=-1}");
        assertThat(recordAndPos2.toString())
                .isEqualTo("RecordAndPos{scanRecord=+A(1,null,3)@0, readRecordsCount=3}");
    }
}
