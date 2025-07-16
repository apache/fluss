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
import java.util.Objects;

/** Represents a product with an id, name, description, price, and category. */
public class Product {

    private long id;
    private String name;
    private String description;
    private BigDecimal price;
    private String category;

    public long id() {
        return id;
    }

    public String name() {
        return name;
    }

    public String description() {
        return description;
    }

    public BigDecimal price() {
        return price;
    }

    public String category() {
        return category;
    }

    public Product(long id, String name, String description, BigDecimal price, String category) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.price = price;
        this.category = category;
    }

    public static Product of(
            long id, String name, String description, BigDecimal price, String category) {
        return new Product(id, name, description, price, category);
    }

    public static Product create(
            String name, String description, BigDecimal price, String category) {
        long pid = Instant.now().toEpochMilli() ^ System.nanoTime();
        return new Product(pid, name, description, price, category);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Product)) {
            return false;
        }
        Product product = (Product) o;
        return Objects.equals(id, product.id)
                && Objects.equals(name, product.name)
                && Objects.equals(description, product.description)
                && Objects.equals(price, product.price)
                && Objects.equals(category, product.category);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, description, price, category);
    }
}
