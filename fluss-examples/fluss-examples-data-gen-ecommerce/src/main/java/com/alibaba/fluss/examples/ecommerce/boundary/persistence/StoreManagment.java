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
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.examples.ecommerce.boundary.data.CustomerFakerDataGenerator;
import com.alibaba.fluss.examples.ecommerce.boundary.persistence.model.MappingTables;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.DatabaseInfo;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * This class serves as a Data Access Object (DAO) for the e-commerce application. It is responsible
 * for interacting with the database to perform CRUD operations on e-commerce entities such as
 * customers, orders, products, and sales.
 */
public class StoreManagment {
    private static final Logger logger = LoggerFactory.getLogger(StoreManagment.class);
    private final Admin admin;
    private final Connection storageConnection;

    public StoreManagment(Connection storageConnection) {
        this.admin = storageConnection.getAdmin();
        this.storageConnection = storageConnection;
    }

    public static StoreManagment setupWith(Connection connection) {
        logger.info("Initializing StoreManagment with connection: {}", connection);
        return new StoreManagment(connection);
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

    public void setupTable(TableDescriptor descriptor, TablePath tablePath)
            throws ExecutionException, InterruptedException {
        logger.info("Setting up table: {} with descriptor: {}", tablePath, descriptor);

        if (admin.tableExists(tablePath).get()) {
            logger.info("Table {} already exists, and will be dropped.", tablePath);
            admin.dropTable(tablePath, true).get();
            Thread.sleep(1000); // Wait for the table to be dropped
            return;
        }

        admin.createTable(tablePath, descriptor, true).get();
        logger.info("Wainting table {} creation...", tablePath);
        Thread.sleep(10000); // Wait for the table to be dropped
        logger.info("Table {} created successfully.", tablePath);

        if (!admin.tableExists(tablePath).get()) {
            logger.error("Failed to create table: {}", tablePath);
            return;
        }

        Table table = storageConnection.getTable(tablePath);
        logger.info("Table {} is ready for use.", tablePath);
        Thread.sleep(1000); // Wait for the table to be dropped
    }

    public void writingCustomerData(TablePath pathPath, int customerCount) {

        Table table = storageConnection.getTable(pathPath);
        AppendWriter writer = table.newAppend().createWriter();

        CustomerFakerDataGenerator dataGenerator = new CustomerFakerDataGenerator();
        dataGenerator
                .generateMany(customerCount)
                .forEach(
                        reading -> {
                            GenericRow row = MappingTables.ofCustomer(reading);
                            writer.append(row);
                        });
        writer.flush();

        logger.info("Customer data Written Successfully.");

        logger.info("Sensor Information Successfully.");
    }

    public void writingBasicCustomerData(TablePath pathPath, int customerCount) {

        Table table = storageConnection.getTable(pathPath);
        AppendWriter writer = table.newAppend().createWriter();

        CustomerFakerDataGenerator dataGenerator = new CustomerFakerDataGenerator();
        dataGenerator
                .generateMany(customerCount)
                .forEach(
                        reading -> {
                            GenericRow row = MappingTables.ofCustomer(reading);
                            writer.append(row);
                        });

        try {
            writer.flush();
            //            admin.close();
            //            storageConnection.close();

            logger.info("Basic Customer data Written Successfully.");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void readingCustomerData(TablePath pathPath) {
        Table table = storageConnection.getTable(pathPath);

        LogScanner logScanner =
                table.newScan()
                        .project(
                                List.of(
                                        "id",
                                        "name",
                                        "address",
                                        "city",
                                        "birthdate",
                                        "createdAt",
                                        "updatedAt"))
                        .createLogScanner();

        int numBuckets = table.getTableInfo().getNumBuckets();
        for (int i = 0; i < numBuckets; i++) {
            logger.info("Subscribing to Bucket {}.", i);
            logScanner.subscribeFromBeginning(i);
        }

        while (true) {
            logger.info("Polling for records...");
            ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
            for (TableBucket bucket : scanRecords.buckets()) {
                for (ScanRecord record : scanRecords.records(bucket)) {
                    InternalRow row = record.getRow();
                    logger.info("Received customer data '{}' ", row);
                    logger.info("---------------------------------------");
                }
            }
        }
    }
}
