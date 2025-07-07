/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.utils;

import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ExponentialBackoff}. */
public class ExponentialBackoffTest {
    @Test
    public void testExponentialBackoff() {
        long initialValue = 100;
        int ratio = 2;
        long backoffMax = 2000;
        double jitter = 0.2;
        ExponentialBackoff exponentialBackoff =
                new ExponentialBackoff(initialValue, ratio, backoffMax, jitter);

        for (int i = 0; i <= 100; i++) {
            for (int attempts = 0; attempts <= 10; attempts++) {
                if (attempts <= 4) {
                    assertThat(1.0 * exponentialBackoff.backoff(attempts))
                            .isCloseTo(
                                    initialValue * Math.pow(ratio, attempts),
                                    Percentage.withPercentage(jitter * 100));
                } else {
                    assertThat(exponentialBackoff.backoff(attempts) <= backoffMax * (1 + jitter))
                            .isTrue();
                }
            }
        }
    }

    @Test
    public void testExponentialBackoffWithoutJitter() {
        ExponentialBackoff exponentialBackoff = new ExponentialBackoff(100, 2, 400, 0.0);
        assertThat(exponentialBackoff.backoff(0)).isEqualTo(100);
        assertThat(exponentialBackoff.backoff(1)).isEqualTo(200);
        assertThat(exponentialBackoff.backoff(2)).isEqualTo(400);
        assertThat(exponentialBackoff.backoff(3)).isEqualTo(400);
    }
}
