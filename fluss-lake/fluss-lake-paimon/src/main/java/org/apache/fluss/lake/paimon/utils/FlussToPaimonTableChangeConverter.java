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

import org.apache.fluss.metadata.TableChange;

import org.apache.paimon.schema.SchemaChange;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/** Converter for {@link TableChange} to {@link SchemaChange}. */
public class FlussToPaimonTableChangeConverter {

    public static List<SchemaChange> convert(
            List<TableChange> tableChanges, Function<String, String> keyFun) {
        List<SchemaChange> schemaChanges = new ArrayList<>(tableChanges.size());

        for (TableChange tableChange : tableChanges) {
            if (tableChange instanceof TableChange.SetOption) {
                TableChange.SetOption setOption = (TableChange.SetOption) tableChange;
                schemaChanges.add(
                        SchemaChange.setOption(
                                keyFun.apply(setOption.getKey()), setOption.getValue()));
            } else if (tableChange instanceof TableChange.ResetOption) {
                TableChange.ResetOption resetOption = (TableChange.ResetOption) tableChange;
                schemaChanges.add(SchemaChange.removeOption(keyFun.apply(resetOption.getKey())));
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported table change: " + tableChange.getClass());
            }
        }

        return schemaChanges;
    }
}
