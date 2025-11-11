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

package org.apache.fluss.lake.paimon.utils;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.TableAlreadyExistException;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.Schema;

import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.FLUSS_CONF_PREFIX;

/** Validator of Paimon table schema. */
public class PaimonSchemaValidation {

    public static void validatePaimonSchemaCapability(Schema existingSchema, Schema newSchema) {
        // check fields
        if (!existingSchema.fields().equals(newSchema.fields())) {
            throw new TableAlreadyExistException(
                    String.format(
                            "The fields of the existing Paimon table are not compatible with those of the new table to be created. "
                                    + "Existing fields: %s, new fields: %s.",
                            existingSchema.fields(), newSchema.fields()));
        }

        // check pks
        if (!existingSchema.primaryKeys().equals(newSchema.primaryKeys())) {
            throw new TableAlreadyExistException(
                    String.format(
                            "The primary keys of the existing Paimon table are not compatible with those of the new table to be created. "
                                    + "Existing primary keys: %s, new primary keys: %s.",
                            existingSchema.primaryKeys(), newSchema.primaryKeys()));
        }

        // check partition keys
        if (!existingSchema.partitionKeys().equals(newSchema.partitionKeys())) {
            throw new TableAlreadyExistException(
                    String.format(
                            "The partition keys of the existing Paimon table are not compatible with those of the new table to be created. "
                                    + "Existing partition keys: %s, new partition keys: %s.",
                            existingSchema.partitionKeys(), newSchema.partitionKeys()));
        }

        // check options
        Map<String, String> existingOptions = new HashMap<>(existingSchema.options());
        Map<String, String> newOptions = new HashMap<>(newSchema.options());
        // `path` will be set automatically by Paimon, so we need to remove it here
        existingOptions.remove(CoreOptions.PATH.key());
        // when enable datalake with an existing table, we should ignore the
        // `table.datalake.enabled`
        String datalakeConfigKey = FLUSS_CONF_PREFIX + ConfigOptions.TABLE_DATALAKE_ENABLED.key();
        if (Boolean.FALSE.toString().equals(existingOptions.get(datalakeConfigKey))) {
            existingOptions.remove(datalakeConfigKey);
            newOptions.remove(datalakeConfigKey);
        }
        if (!existingOptions.equals(newOptions)) {
            throw new TableAlreadyExistException(
                    String.format(
                            "The options of the existing Paimon table are not compatible with those of the new table to be created. "
                                    + "Existing options: %s, new options: %s.",
                            existingOptions, newSchema.options()));
        }
    }
}
