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

package com.alibaba.fluss.examples.ecommerce.boundary.data;

import com.alibaba.fluss.examples.ecommerce.control.datageneration.DataGenerator;
import com.alibaba.fluss.examples.ecommerce.entity.Customer;
import com.alibaba.fluss.examples.ecommerce.entity.Order;
import com.alibaba.fluss.examples.ecommerce.entity.Product;

import net.datafaker.Faker;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Repository interface for managing Customer entities. This interface extends the generic
 * Repository interface.
 */
public class OrderFakerDataGenerator implements DataGenerator<Order> {
    private final List<Customer> customers;
    private final List<Product> products;
    private List<Order> orders;

    public OrderFakerDataGenerator(List<Customer> customers, List<Product> products) {
        this.customers = customers;
        this.products = products;
    }

    @Override
    public Order generateOne() {
        generateMany(1);
        return orders.stream().findFirst().orElse(null);
    }

    @Override
    public void generateMany(int count) {
        Customer customer = customers.get(new Random().nextInt(customers.size()));
        Product product = products.get(new Random().nextInt(products.size()));

        orders =
                faker.collection(
                                () -> {
                                    long quantity = faker.number().numberBetween(1, 100);
                                    BigDecimal amount = product.price();

                                    LocalDateTime orderDate =
                                            faker.date()
                                                    .past(1000, TimeUnit.DAYS)
                                                    .toLocalDateTime();

                                    return Order.create(
                                            customer.id(),
                                            quantity,
                                            product.id(),
                                            orderDate,
                                            amount);
                                })
                        .maxLen(new Random().nextInt(count))
                        .generate();
    }

    public List<Order> getData() {
        return orders;
    }

    private final Faker faker = new Faker();
}
