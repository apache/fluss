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

package com.alibaba.fluss.server.entity;

import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.metadata.UpdateProperties;
import com.alibaba.fluss.rpc.messages.AlterTableRequest;

/** The data for request {@link AlterTableRequest}. */
public class AlterTableData {
    private final TablePath tablePath;
    private final UpdateProperties updateProperties;
    // TODO add more alter table type like add column, change schema etc.
    public AlterTableData(TablePath tablePath, UpdateProperties updateProperties) {
        this.tablePath = tablePath;
        this.updateProperties = updateProperties;
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public UpdateProperties getUpdatePropertiesData() {
        return updateProperties;
    }
}
