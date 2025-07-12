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
import com.alibaba.fluss.examples.ecommerce.entity.Product;

import net.datafaker.Faker;

import java.util.List;

/**
 * Repository interface for managing Customer entities. This interface extends the generic
 * Repository interface.
 */
public class ProductFakerDataGenerator implements DataGenerator<Product> {
    @Override
    public Product generateOne() {
        return generateMany(1).stream().findFirst().orElse(null);
    }

    @Override
    public List<Product> generateMany(int count) {
        return faker.collection(
                        () -> {
                            String name = faker.commerce().productName();
                            Double price = Double.valueOf(faker.commerce().price(1.50, 30000.60));
                            String description =
                                    String.format(
                                            "Brand: %s Department: %s Material: %s, Vendor: %s",
                                            faker.commerce().brand(),
                                            faker.commerce().department(),
                                            faker.commerce().material(),
                                            faker.commerce().vendor());
                            String category = faker.commerce().department();

                            return Product.create(name, description, price, category);
                        })
                .maxLen(count)
                .generate();
    }

    private final Faker faker = new Faker();
}
