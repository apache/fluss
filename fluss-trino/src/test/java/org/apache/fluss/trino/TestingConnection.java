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

package org.apache.fluss.trino;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.MultiTable;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;

/** Delegates to a real connection while allowing individual failure paths to be exercised. */
class TestingConnection implements Connection {
    private final Connection delegate;

    TestingConnection(Connection delegate) {
        this.delegate = delegate;
    }

    @Override
    public Configuration getConfiguration() {
        return delegate.getConfiguration();
    }

    @Override
    public Admin getAdmin() {
        return delegate.getAdmin();
    }

    @Override
    public Table getTable(TablePath tablePath) {
        return delegate.getTable(tablePath);
    }

    @Override
    public MultiTable getMultiTable() {
        return delegate.getMultiTable();
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }
}
