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

package com.alibaba.fluss.examples.ecommerce.boundary.persistence.model;

import com.alibaba.fluss.examples.ecommerce.entity.Customer;
import com.alibaba.fluss.examples.ecommerce.entity.Product;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.TimestampNtz;

/**
 * This class provides methods to convert domain entities into Fluss GenericRow objects for
 * persistence in the database.
 */
public class MappingTables {
    public static GenericRow ofCustomer(Customer customer) {
        GenericRow row = new GenericRow(Customer.class.getDeclaredFields().length);
        row.setField(0, customer.id());
        row.setField(1, BinaryString.fromString(customer.name()));
        row.setField(2, (int) customer.birthdate().toEpochDay());
        row.setField(3, BinaryString.fromString(customer.address()));
        row.setField(4, BinaryString.fromString(customer.city()));
        row.setField(5, TimestampNtz.fromLocalDateTime(customer.createdAt()));
        row.setField(6, TimestampNtz.fromLocalDateTime(customer.updatedAt()));
        return row;
    }

    public static GenericRow ofProduct(Product product) {
        GenericRow row = new GenericRow(Product.class.getDeclaredFields().length);

        return row;
    }
}
