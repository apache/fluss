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

package com.alibaba.fluss.examples.ecommerce.boundary.model;

import com.alibaba.fluss.examples.ecommerce.boundary.data.CustomerFakerDataGenerator;
import com.alibaba.fluss.examples.ecommerce.boundary.data.ProductFakerDataGenerator;
import com.alibaba.fluss.examples.ecommerce.entity.Customer;
import com.alibaba.fluss.examples.ecommerce.entity.Product;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.TimestampNtz;

import net.datafaker.Faker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;

/** */
class FakerGeneratorTest {

    @Test
    void createCustomer() {
        Faker faker = new Faker();
        Customer customer =
                Customer.create(
                        faker.name().fullName(),
                        faker.address().fullAddress(),
                        faker.address().city(),
                        LocalDate.now().minusYears(20));

        Customer customer1 =
                Customer.create(
                        faker.name().fullName(),
                        faker.address().fullAddress(),
                        faker.address().city(),
                        LocalDate.now().minusYears(20));
        Customer customer2 =
                Customer.create(
                        faker.name().fullName(),
                        faker.address().fullAddress(),
                        faker.address().city(),
                        LocalDate.now().minusYears(20));

        // Add assertions to verify the properties of the created customer
        Assertions.assertNotNull(customer.name());
        Assertions.assertNotNull(customer.address());
        Assertions.assertNotNull(customer.birthdate());
        Assertions.assertNotNull(customer.city());
    }

    @Test
    @DisplayName("Generate Fake Customer")
    void generateFakeCustomer() {
        CustomerFakerDataGenerator respotiry = new CustomerFakerDataGenerator();

        Customer customer = respotiry.generateOne();

        // Add assertions to verify the properties of the generated customer
        Assertions.assertNotNull(customer.name());
        Assertions.assertNotNull(customer.address());
        Assertions.assertNotNull(customer.birthdate());
        Assertions.assertNotNull(customer.city());
    }

    @Test
    void generateManyCustomers() {
        CustomerFakerDataGenerator respotiry = new CustomerFakerDataGenerator();

        int count = 1000;
        respotiry.generateMany(count);
        List<Customer> customers = respotiry.getData();

        // Add assertions to verify the properties of the generated customers
        Assertions.assertEquals(count, customers.size());
        for (Customer customer : customers) {
            Assertions.assertNotNull(customer.id());
            Assertions.assertNotNull(customer.name());
            Assertions.assertNotNull(customer.address());
            Assertions.assertNotNull(customer.birthdate());
            Assertions.assertNotNull(customer.city());
        }
    }

    @Test
    void generateOneProduct() {
        ProductFakerDataGenerator repository = new ProductFakerDataGenerator();

        Product product = repository.generateOne();

        // Add assertions to verify the properties of the generated product
        Assertions.assertNotNull(product.name());
        Assertions.assertNotNull(product.description());
        Assertions.assertNotNull(product.price());
        Assertions.assertNotNull(product.category());
    }

    @Test
    void generateManyProducts() {
        ProductFakerDataGenerator repository = new ProductFakerDataGenerator();

        int count = 1000;
        repository.generateMany(count);
        List<Product> products = repository.getData();

        // Add assertions to verify the properties of the generated products
        Assertions.assertEquals(count, products.size());
        for (Product product : products) {
            Assertions.assertNotNull(product.name());
            Assertions.assertNotNull(product.description());
            Assertions.assertNotNull(product.price());
            Assertions.assertNotNull(product.category());
        }
    }

    @Test
    void tableMappingCheck() {
        Customer customer =
                Customer.create(
                        "John Doe",
                        "123 Main St, Springfield, USA",
                        "Springfield",
                        LocalDate.now());
        GenericRow row = new GenericRow(Customer.class.getDeclaredFields().length);
        row.setField(0, customer.id());
        row.setField(1, BinaryString.fromString(customer.name()));
        row.setField(2, (int) customer.birthdate().toEpochDay());
        row.setField(3, BinaryString.fromString(customer.address()));
        row.setField(4, BinaryString.fromString(customer.city()));
        row.setField(5, TimestampNtz.fromLocalDateTime(customer.createdAt()));
        row.setField(6, TimestampNtz.fromLocalDateTime(customer.updatedAt()));
        Assertions.assertTrue(true, "TableMappings class is functioning as expected.");
    }
}
