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

package com.alibaba.fluss.examples.ecommerce.boundary.streams.producers;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.examples.ecommerce.boundary.persistence.model.MappingTables;
import com.alibaba.fluss.examples.ecommerce.boundary.persistence.model.Tables;
import com.alibaba.fluss.examples.ecommerce.boundary.streams.Operation;
import com.alibaba.fluss.examples.ecommerce.control.datageneration.DataGenerator;
import com.alibaba.fluss.examples.ecommerce.entity.Customer;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

/**
 * This class serves as a Data Access Object (DAO) for the e-commerce application. It is responsible
 * for interacting with the database to perform CRUD operations on e-commerce entities such as
 * customers, orders, products, and sales.
 */
public class CustomerProducer extends Operation {
    private static final Logger logger = LoggerFactory.getLogger(CustomerProducer.class);
    private final UpsertWriter upsertWriter;
    private final DataGenerator<Customer> datagenerator;

    public CustomerProducer(Connection storageConnection, DataGenerator<Customer> datagenerator) {
        super(storageConnection, Tables.CUSTOMER_TABLE_PATH);
        this.upsertWriter = produceToTable.newUpsert().createWriter();
        this.datagenerator = datagenerator;
    }

    public static CustomerProducer setupWith(
            Connection connection, DataGenerator<Customer> generator) {

        logger.info("Initializing producer with connection: {}", connection);

        return new CustomerProducer(connection, generator);
    }

    public void produceCustomerData(int customerToProduce) {
        try {
            datagenerator.generateMany(customerToProduce);
            datagenerator
                    .getData()
                    .forEach(
                            customer -> {
                                GenericRow row = MappingTables.ofCustomer(customer);
                                upsertWriter.upsert(row);
                            });

            upsertWriter.flush();

            logger.info("Customer data Successfully written.");
        } catch (Exception e) {
            logger.error("Error customer {0}", e);
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
