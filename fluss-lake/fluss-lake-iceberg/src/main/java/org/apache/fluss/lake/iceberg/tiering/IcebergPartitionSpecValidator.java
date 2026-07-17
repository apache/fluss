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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.lake.iceberg.IcebergSchemaUtils;
import org.apache.fluss.lake.iceberg.utils.IcebergPartitionSpecUtils;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;

import java.util.List;

/** Validates that an Iceberg partition spec is compatible with Fluss lake tiering. */
final class IcebergPartitionSpecValidator {

    private IcebergPartitionSpecValidator() {}

    static void validate(Table icebergTable, TableInfo tableInfo) {
        TableDescriptor tableDescriptor = tableInfo.toTableDescriptor();
        Schema icebergSchema = icebergTable.schema();
        Schema expectedSchema =
                IcebergSchemaUtils.createIcebergSchema(tableDescriptor, tableInfo.hasPrimaryKey());
        if (!IcebergSchemaUtils.compatibleWith(icebergSchema, expectedSchema)) {
            throw new InvalidTableException(
                    String.format(
                            "Iceberg schema is incompatible with Fluss table %s. "
                                    + "Expected Iceberg schema: %s, but the current Iceberg schema is: %s.",
                            tableInfo.getTablePath(), expectedSchema, icebergSchema));
        }

        PartitionSpec icebergSpec = icebergTable.spec();
        PartitionSpec expectedSpec =
                IcebergPartitionSpecUtils.createPartitionSpec(tableDescriptor, icebergSchema);

        if (!compatibleWith(icebergSpec, expectedSpec)) {
            throw new InvalidTableException(
                    String.format(
                            "Iceberg partition spec is incompatible with Fluss table %s. "
                                    + "Expected Iceberg partition spec: %s, but the current Iceberg partition spec is: %s.",
                            tableInfo.getTablePath(), expectedSpec, icebergSpec));
        }
    }

    private static boolean compatibleWith(PartitionSpec currentSpec, PartitionSpec expectedSpec) {
        List<PartitionField> currentFields = currentSpec.fields();
        List<PartitionField> expectedFields = expectedSpec.fields();
        if (currentFields.size() != expectedFields.size()) {
            return false;
        }

        for (int i = 0; i < currentFields.size(); i++) {
            PartitionField currentField = currentFields.get(i);
            PartitionField expectedField = expectedFields.get(i);
            if (currentField.sourceId() != expectedField.sourceId()
                    || !currentField.transform().equals(expectedField.transform())) {
                return false;
            }
        }
        return true;
    }
}
