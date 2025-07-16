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
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Represents a sale transaction with details such as id, order id, product id, sale date, and
 * amount.
 */
public class Sale {
    private final long id;
    private final long orderId;
    private final long productId;
    private final LocalDateTime saleDate;
    private final BigDecimal amount;

    public long id() {
        return id;
    }

    public long orderId() {
        return orderId;
    }

    public long productId() {
        return productId;
    }

    public LocalDateTime saleDate() {
        return saleDate;
    }

    public BigDecimal amount() {
        return amount;
    }

    public Sale(long id, long orderId, long productId, LocalDateTime saleDate, BigDecimal amount) {
        this.id = id;
        this.orderId = orderId;
        this.productId = productId;
        this.saleDate = saleDate;
        this.amount = amount;
    }

    public static Sale of(
            long id, long orderId, long productId, LocalDateTime saleDate, BigDecimal amount) {
        return new Sale(id, orderId, productId, saleDate, amount);
    }

    public static Sale create(
            long orderId, long productId, LocalDateTime saleDate, BigDecimal amount) {
        long sid = System.currentTimeMillis();
        return new Sale(sid, orderId, productId, saleDate, amount);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Sale)) {
            return false;
        }
        Sale sale = (Sale) o;
        return Objects.equals(id, sale.id)
                && Objects.equals(orderId, sale.orderId)
                && Objects.equals(productId, sale.productId)
                && Objects.equals(saleDate, sale.saleDate)
                && Objects.equals(amount, sale.amount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, orderId, productId, saleDate, amount);
    }
}
