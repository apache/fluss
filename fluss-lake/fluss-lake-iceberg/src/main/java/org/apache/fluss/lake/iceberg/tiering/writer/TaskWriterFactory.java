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

package org.apache.fluss.lake.iceberg.tiering.writer;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.util.PropertyUtil;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

/** A factory to create Iceberg {@link TaskWriter}. */
public class TaskWriterFactory {

    public static TaskWriter<Record> createTaskWriter(
            Table table, @Nullable String partition, int bucket) {
        Schema schema = table.schema();
        int[] equalityFieldIds =
                schema.identifierFieldIds().stream().mapToInt(Integer::intValue).toArray();

        // Get target file size from table properties
        long targetFileSize = targetFileSize(table);
        FileFormat format = fileFormat(table);
        OutputFileFactory outputFileFactory =
                OutputFileFactory.builderFor(
                                table,
                                bucket,
                                // task id always 0
                                0)
                        .format(format)
                        .build();

        if (equalityFieldIds.length == 0) {
            FileAppenderFactory<Record> fileAppenderFactory =
                    new GenericAppenderFactory(schema, table.spec());
            return new GenericRecordAppendOnlyWriter(
                    table,
                    format,
                    fileAppenderFactory,
                    outputFileFactory,
                    table.io(),
                    targetFileSize,
                    partition,
                    bucket);

        } else {
            FileAppenderFactory<Record> appenderFactory =
                    new GenericAppenderFactory(
                            schema, table.spec(), equalityFieldIds, schema, null);
            List<String> columns = new ArrayList<>();
            for (Integer fieldId : schema.identifierFieldIds()) {
                columns.add(schema.findField(fieldId).name());
            }
            Schema deleteSchema = schema.select(columns);
            return new GenericRecordDeltaWriter(
                    table,
                    deleteSchema,
                    format,
                    appenderFactory,
                    outputFileFactory,
                    table.io(),
                    targetFileSize,
                    partition,
                    bucket);
        }
    }

    private static FileFormat fileFormat(Table icebergTable) {
        String formatString =
                PropertyUtil.propertyAsString(
                        icebergTable.properties(),
                        TableProperties.DEFAULT_FILE_FORMAT,
                        TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
        return FileFormat.fromString(formatString);
    }

    private static long targetFileSize(Table icebergTable) {
        return PropertyUtil.propertyAsLong(
                icebergTable.properties(),
                WRITE_TARGET_FILE_SIZE_BYTES,
                WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    }
}
