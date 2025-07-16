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

package com.alibaba.fluss.examples.ecommerce.boundary.persistence;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.DatabaseInfo;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

/**
 * This class serves as a Data Access Object (DAO) for the e-commerce application. It is responsible
 * for interacting with the database to perform CRUD operations on e-commerce entities such as
 * customers, orders, products, and sales.
 */
public class PrepareDatabase {
    private static final Logger logger = LoggerFactory.getLogger(PrepareDatabase.class);
    protected final Admin admin;
    protected final Connection storageConnection;

    public PrepareDatabase(Connection storageConnection) {
        this.admin = storageConnection.getAdmin();
        this.storageConnection = storageConnection;
    }

    public static Connection getConnection(String bootstrapServers) {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), bootstrapServers);

        return ConnectionFactory.createConnection(conf);
    }

    public static PrepareDatabase setupWith(Connection storageConnection) {
        return new PrepareDatabase(storageConnection);
    }

    public void setupDatabase(String dbName, DatabaseDescriptor descriptor)
            throws ExecutionException, InterruptedException {

        if (admin.databaseExists(dbName).get()) {
            logger.info("Database {} already exists, skipping creation.", dbName);
            return;
        }

        admin.createDatabase(dbName, descriptor, false).get();
        logger.info("Database {} created successfully with descriptor: {}", dbName, descriptor);

        DatabaseInfo dbInfo = admin.getDatabaseInfo(dbName).get();
        logger.info("Database info: {}", new String(dbInfo.getDatabaseDescriptor().toJsonBytes()));
    }

    public void setupTable(TableDescriptor descriptor, TablePath tablePath, boolean dropIfExists)
            throws ExecutionException, InterruptedException {
        logger.info("Setting up table: {} with descriptor: {}", tablePath, descriptor);

        if (dropIfExists && admin.tableExists(tablePath).get()) {
            logger.info("Table {} already exists, and will be dropped.", tablePath);
            admin.dropTable(tablePath, true).get();
            Thread.sleep(2000); // Wait for the table to be dropped
        }

        admin.createTable(tablePath, descriptor, true).get();
        logger.info("Wainting table {} creation...", tablePath);

        boolean tableCreated = false;

        while (tableCreated) {
            tableCreated = admin.tableExists(tablePath).get();
            if (tableCreated) {
                logger.info("Table {} created successfully.", tablePath);
                break;
            }

            Thread.sleep(2000); // Wait for the table to be propagated
        }

        TableInfo tableInfo = storageConnection.getTable(tablePath).getTableInfo();
        logger.info("Table details '{}' is ready for use.", tableInfo);
    }
}
