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

import net.datafaker.Faker;

import java.time.LocalDate;
import java.util.List;

/**
 * Repository interface for managing Customer entities. This interface extends the generic
 * Repository interface.
 */
public class CustomerFakerDataGenerator implements DataGenerator<Customer> {
    @Override
    public Customer generateOne() {
        return generateMany(1).stream().findFirst().orElse(null);
    }

    @Override
    public List<Customer> generateMany(int count) {
        return faker.collection(
                        () -> {
                            String name = faker.name().fullName();
                            LocalDate birthDate =
                                    faker.date()
                                            .birthday()
                                            .toLocalDateTime()
                                            .toLocalDate()
                                            .minusYears(faker.number().numberBetween(18, 40));
                            String address = faker.address().fullAddress();
                            String city = faker.address().city();

                            return Customer.create(name, address, city, birthDate);
                        })
                .maxLen(count)
                .generate();
    }

    private final Faker faker = new Faker();
}
