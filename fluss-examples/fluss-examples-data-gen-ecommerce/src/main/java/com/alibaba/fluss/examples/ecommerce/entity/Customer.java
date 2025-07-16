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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Represents a customer with basic personal information. Provides factory methods for creating
 * instances and overrides equals and hashCode.
 */
public class Customer {
    private final long id;
    private final String name;
    private final String address;
    private final LocalDate birthdate;
    private final LocalDateTime updatedAt;
    private final LocalDateTime createdAt;
    private final String city;

    public long id() {
        return id;
    }

    public String name() {
        return name;
    }

    public String address() {
        return address;
    }

    public LocalDate birthdate() {
        return birthdate;
    }

    public String city() {
        return city;
    }

    public LocalDateTime updatedAt() {
        return updatedAt;
    }

    public LocalDateTime createdAt() {
        return createdAt;
    }

    public Customer(
            long id,
            String name,
            String address,
            String city,
            LocalDate birthdate,
            LocalDateTime updatedAt,
            LocalDateTime createdAt) {
        this.id = id;
        this.name = name;
        this.address = address;
        this.birthdate = birthdate;
        this.city = city;
        this.updatedAt = updatedAt;
        this.createdAt = createdAt;
    }

    public static Customer of(
            long id,
            String name,
            String address,
            String city,
            LocalDate birthdate,
            LocalDateTime updatedAt,
            LocalDateTime createdAt) {
        return new Customer(id, name, address, city, birthdate, updatedAt, createdAt);
    }

    public static Customer create(String name, String address, String city, LocalDate birthdate) {
        long cid = (Instant.now().toEpochMilli() ^ System.nanoTime());
        LocalDateTime createdAt = LocalDateTime.now();
        return Customer.of(cid, name, address, city, birthdate, createdAt, createdAt);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Customer)) {
            return false;
        }
        Customer customer = (Customer) o;
        return Objects.equals(id, customer.id)
                && Objects.equals(name, customer.name)
                && Objects.equals(address, customer.address)
                && Objects.equals(birthdate, customer.birthdate)
                && Objects.equals(city, customer.city);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, address, city);
    }

    @Override
    public String toString() {
        return "Customer{"
                + "id='"
                + String.format("%d", id)
                + '\''
                + ", name='"
                + name
                + '\''
                + ", address='"
                + address
                + '\''
                + ", birthdate="
                + birthdate
                + ", updatedAt="
                + updatedAt
                + ", createdAt="
                + createdAt
                + ", city='"
                + city
                + '\''
                + '}';
    }
}
