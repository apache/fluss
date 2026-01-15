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

package org.apache.fluss.lake.values;

import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.lake.lakestorage.LakeCatalog;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;

import java.util.List;

/** Implementation of {@link LakeCatalog} for values lake. */
public class TestingValuesLakeCatalog implements LakeCatalog {
    @Override
    public void createTable(TablePath tablePath, TableDescriptor tableDescriptor, Context context)
            throws TableAlreadyExistException {
        TestingValuesLake.createTable(tablePath.toString());
    }

    @Override
    public void alterTable(TablePath tablePath, List<TableChange> tableChanges, Context context)
            throws TableNotExistException {
        throw new RuntimeException("Not impl.");
    }
}
