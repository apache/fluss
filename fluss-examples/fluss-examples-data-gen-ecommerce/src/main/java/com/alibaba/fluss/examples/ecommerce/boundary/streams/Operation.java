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

package com.alibaba.fluss.examples.ecommerce.boundary.streams;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.metadata.TablePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class serves as a Data Access Object (DAO) for the e-commerce application. It is responsible
 * for interacting with the database to perform CRUD operations on e-commerce entities such as
 * customers, orders, products, and sales.
 */
public abstract class Operation {
    private static final Logger logger = LoggerFactory.getLogger(Operation.class);
    protected final Connection storageConnection;
    protected final Table produceToTable;

    public Operation(Connection storageConnection, TablePath tablePath) {
        this.storageConnection = storageConnection;
        this.produceToTable = storageConnection.getTable(tablePath);
    }
}
