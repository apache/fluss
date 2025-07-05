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

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Represents an order placed by a customer. Contains order id, customer id, order date, and total
 * amount.
 */
public class Order {
    private long id;
    private long customerId;
    private LocalDateTime orderDate;
    private Double total;

    public long id() {
        return id;
    }

    public long customerId() {
        return customerId;
    }

    public LocalDateTime orderDate() {
        return orderDate;
    }

    public Double total() {
        return total;
    }

    public Order(long id, long customerId, LocalDateTime orderDate, Double total) {
        this.id = id;
        this.customerId = customerId;
        this.orderDate = orderDate;
        this.total = total;
    }

    public static Order of(long id, long customerId, LocalDateTime orderDate, Double total) {
        return new Order(id, customerId, orderDate, total);
    }

    public static Order create(long customerId, String orderDate, Double total) {
        long oid = System.currentTimeMillis();
        return new Order(oid, customerId, LocalDateTime.parse(orderDate), total);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Order)) {
            return false;
        }
        Order order = (Order) o;
        return Objects.equals(id, order.id)
                && Objects.equals(customerId, order.customerId)
                && Objects.equals(orderDate, order.orderDate)
                && Objects.equals(total, order.total);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, customerId, orderDate, total);
    }
}
