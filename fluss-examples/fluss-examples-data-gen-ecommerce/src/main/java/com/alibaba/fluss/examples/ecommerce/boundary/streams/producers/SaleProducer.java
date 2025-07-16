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
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.examples.ecommerce.boundary.persistence.model.MappingTables;
import com.alibaba.fluss.examples.ecommerce.boundary.persistence.model.Tables;
import com.alibaba.fluss.examples.ecommerce.boundary.streams.Operation;
import com.alibaba.fluss.examples.ecommerce.control.datageneration.DataGenerator;
import com.alibaba.fluss.examples.ecommerce.entity.Sale;
import com.alibaba.fluss.row.GenericRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class serves as a Data Access Object (DAO) for the e-commerce application. It is responsible
 * for interacting with the database to perform CRUD operations on e-commerce entities such as
 * customers, orders, products, and sales.
 */
public class SaleProducer extends Operation {
    private static final Logger logger = LoggerFactory.getLogger(SaleProducer.class);
    private final AppendWriter appendWriter;
    private final DataGenerator<Sale> datagenerator;

    public SaleProducer(Connection storageConnection, DataGenerator<Sale> datagenerator) {
        super(storageConnection, Tables.SALE_TABLE_PATH);
        appendWriter = produceToTable.newAppend().createWriter();
        this.datagenerator = datagenerator;
    }

    public static SaleProducer setupWith(Connection connection, DataGenerator<Sale> datagenerator) {

        logger.info("Initializing producer with connection: {}", connection);

        return new SaleProducer(connection, datagenerator);
    }

    public void produceOrderData(int saleToProduce) {
        try {
            datagenerator.generateMany(saleToProduce);
            datagenerator
                    .getData()
                    .forEach(
                            sale -> {
                                GenericRow row = MappingTables.ofSale(sale);
                                appendWriter.append(row);
                            });

            appendWriter.flush();

            logger.info("Sale data Successfully written.");

        } catch (Exception e) {
            logger.error("Error while writing Sale data: ", e);
        }
    }
}
