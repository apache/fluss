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

package com.alibaba.fluss.examples.ecommerce.entity;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Represents an order placed by a customer. Contains order id, customer id, order date, and total
 * amount.
 */
public class Order {
    private final long id;
    private final long customerId;
    private final long quantity;
    private final long productId;
    private final LocalDateTime orderDate;
    private final BigDecimal amount;

    public long id() {
        return id;
    }

    public long customerId() {
        return customerId;
    }

    public long quantity() {
        return quantity;
    }

    public long productId() {
        return productId;
    }

    public LocalDateTime orderDate() {
        return orderDate;
    }

    public BigDecimal amount() {
        return amount;
    }

    public Order(
            long id,
            long customerId,
            long quantity,
            long productId,
            LocalDateTime orderDate,
            BigDecimal amount) {
        this.id = id;
        this.customerId = customerId;
        this.quantity = quantity;
        this.productId = productId;
        this.orderDate = orderDate;
        this.amount = amount;
    }

    public static Order of(
            long id,
            long customerId,
            long quantity,
            long productId,
            LocalDateTime orderDate,
            BigDecimal amount) {
        return new Order(id, customerId, quantity, productId, orderDate, amount);
    }

    public static Order create(
            long customerId,
            long quantity,
            long productId,
            LocalDateTime orderDate,
            BigDecimal amount) {
        long oid = Instant.now().toEpochMilli() ^ System.nanoTime();
        return new Order(oid, customerId, quantity, productId, orderDate, amount);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Order)) {
            return false;
        }
        Order order = (Order) o;
        return Objects.equals(id, order.id)
                && Objects.equals(customerId, order.customerId)
                && Objects.equals(productId, order.productId)
                && Objects.equals(orderDate, order.orderDate)
                && Objects.equals(amount, order.amount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, customerId, quantity, productId, orderDate, amount);
    }
}
