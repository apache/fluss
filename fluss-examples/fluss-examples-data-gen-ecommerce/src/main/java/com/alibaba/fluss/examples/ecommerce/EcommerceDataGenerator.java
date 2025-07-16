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

package com.alibaba.fluss.examples.ecommerce;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.examples.ecommerce.boundary.data.CustomerFakerDataGenerator;
import com.alibaba.fluss.examples.ecommerce.boundary.data.OrderFakerDataGenerator;
import com.alibaba.fluss.examples.ecommerce.boundary.data.ProductFakerDataGenerator;
import com.alibaba.fluss.examples.ecommerce.boundary.data.SaleFakerDataGenerator;
import com.alibaba.fluss.examples.ecommerce.boundary.persistence.PrepareDatabase;
import com.alibaba.fluss.examples.ecommerce.boundary.persistence.model.Descriptors;
import com.alibaba.fluss.examples.ecommerce.boundary.persistence.model.Tables;
import com.alibaba.fluss.examples.ecommerce.boundary.streams.producers.CustomerProducer;
import com.alibaba.fluss.examples.ecommerce.boundary.streams.producers.OrderProducer;
import com.alibaba.fluss.examples.ecommerce.boundary.streams.producers.ProductProducer;
import com.alibaba.fluss.examples.ecommerce.boundary.streams.producers.SaleProducer;
import com.alibaba.fluss.examples.ecommerce.control.datageneration.DataGenerator;
import com.alibaba.fluss.examples.ecommerce.entity.Customer;
import com.alibaba.fluss.examples.ecommerce.entity.Order;
import com.alibaba.fluss.examples.ecommerce.entity.Product;
import com.alibaba.fluss.examples.ecommerce.entity.Sale;

import org.slf4j.Logger;

import java.util.concurrent.ExecutionException;

/**
 * This class serves as the entry point for the E-commerce Data Generator application. It
 * initializes the application and sets up the necessary components for generating e-commerce data.
 */
public class EcommerceDataGenerator {
    private static Logger logger = org.slf4j.LoggerFactory.getLogger(EcommerceDataGenerator.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        logger.info("E-commerce Data Generator is starting...");

        String bootstrapServers =
                "localhost:9123,localhost:9124,localhost:9125"; // Example bootstrap server

        if (args.length > 0) {
            logger.info("Default server host changed via CLI to: {}", args[0]);
            bootstrapServers = args[0];
        } else if (System.getenv("FLUSS_HOST") != null) {
            logger.info("Default server host changed via ENV to: {}", System.getenv("FLUSS_HOST"));
            bootstrapServers = System.getenv("FLUSS_HOST");
        }

        Connection connection = PrepareDatabase.getConnection(bootstrapServers);

        PrepareDatabase management = PrepareDatabase.setupWith(connection);

        management.setupDatabase(Descriptors.DB_NAME, Descriptors.ECOMMERCE_DB_DESCRIPTOR);

        management.setupTable(
                Descriptors.CUSTOMER_TABLE_DESCRIPTOR, Tables.CUSTOMER_TABLE_PATH, true);
        management.setupTable(
                Descriptors.PRODUCT_TABLE_DESCRIPTOR, Tables.PRODUCT_TABLE_PATH, true);

        management.setupTable(Descriptors.ORDER_TABLE_DESCRIPTOR, Tables.ORDER_TABLE_PATH, true);

        management.setupTable(Descriptors.SALE_TABLE_DESCRIPTOR, Tables.SALE_TABLE_PATH, true);

        logger.info("E-commerce Data Generator setup completed successfully.");

        int customersToGenerate = 10000; // Example customer count
        logger.info("Let's Generate {} Customers.", customersToGenerate);

        DataGenerator<Customer> cusGenerator = new CustomerFakerDataGenerator();
        CustomerProducer cusProducer = CustomerProducer.setupWith(connection, cusGenerator);
        cusProducer.produceCustomerData(customersToGenerate);

        logger.info("{} Customers written to database.", customersToGenerate);

        DataGenerator<Product> prdGenerator = new ProductFakerDataGenerator();
        ProductProducer prodProducer = ProductProducer.setupWith(connection, prdGenerator);
        int productToGenerate = 1000;
        prodProducer.produceProductData(productToGenerate);

        logger.info("{} Products written to database.", productToGenerate);

        DataGenerator<Order> ordGenerator =
                new OrderFakerDataGenerator(cusGenerator.getData(), prdGenerator.getData());
        int orderToGenerate = 1000000;
        OrderProducer ordProducer =
                OrderProducer.setupWith(
                        connection, cusGenerator.getData(), prdGenerator.getData(), ordGenerator);
        ordProducer.produceOrderData(orderToGenerate);
        logger.info("{} Orders written to database.", orderToGenerate);

        DataGenerator<Sale> sls = new SaleFakerDataGenerator(ordGenerator.getData());
        int salesToGenerate = 900000;
        SaleProducer slsProducer = SaleProducer.setupWith(connection, sls);
        slsProducer.produceOrderData(salesToGenerate);
        logger.info("{} Sales written to database.", salesToGenerate);
    }
}
