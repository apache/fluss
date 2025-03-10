/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.utils.types;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EitherTest {

    @Test
    void testEither() {
        // test equal
        Either<String, String> left1 = Either.left("left1");
        Either<String, String> left2 = Either.Left.of("left1");
        Either<String, String> right1 = Either.right("right1");
        Either<String, String> right2 = Either.Right.of("right1");
        assertThat(left1).isNotEqualTo(right1);
        assertThat(left1).isEqualTo(left2);
        assertThat(right1).isNotEqualTo(left1);
        assertThat(right1).isEqualTo(right2);

        // test left & right value
        assertThat(left1.left()).isEqualTo("left1");
        assertThat(right1.right()).isEqualTo("right1");

        // test to string
        assertThat(left1.toString()).isEqualTo("Left(left1)");
        assertThat(right1.toString()).isEqualTo("Right(right1)");

        // test is right/left
        assertThat(left1.isLeft()).isTrue();
        assertThat(left1.isRight()).isFalse();
        assertThat(right1.isLeft()).isFalse();
        assertThat(right1.isRight()).isTrue();

        // should throw exception when try to access other side
        assertThatThrownBy(left1::right).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(right1::left).isInstanceOf(IllegalStateException.class);
    }
}
