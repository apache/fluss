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

import com.alibaba.fluss.annotation.PublicStable;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This type represents a value of one two possible types, Left or Right (a disjoint union),
 * inspired by Scala's Either type.
 *
 * @param <L> the type of Left
 * @param <R> the type of Right
 * @since 0.1
 */
@PublicStable
public abstract class Either<L, R> {

    /** Create a Left value of Either. */
    public static <L, R> Either<L, R> left(L value) {
        return new Left<L, R>(value);
    }

    /** Create a Right value of Either. */
    public static <L, R> Either<L, R> right(R value) {
        return new Right<L, R>(value);
    }

    /**
     * Retrieve the Left value of Either.
     *
     * @return the Left value
     * @throws IllegalStateException if called on a Right
     */
    public abstract L left() throws IllegalStateException;

    /**
     * Retrieve the Right value of Either.
     *
     * @return the Right value
     * @throws IllegalStateException if called on a Left
     */
    public abstract R right() throws IllegalStateException;

    /** @return true if this is a Left value, false if this is a Right value */
    public final boolean isLeft() {
        return getClass() == Left.class;
    }

    /** @return true if this is a Right value, false if this is a Left value */
    public final boolean isRight() {
        return getClass() == Right.class;
    }

    /**
     * A left value of {@link Either}.
     *
     * @param <L> the type of Left
     * @param <R> the type of Right
     */
    public static class Left<L, R> extends Either<L, R> {
        private L value;

        private Right<L, R> right;

        public Left(L value) {
            this.value = java.util.Objects.requireNonNull(value);
        }

        @Override
        public L left() {
            return value;
        }

        @Override
        public R right() {
            throw new IllegalStateException("Cannot retrieve Right value on a Left");
        }

        /**
         * Sets the encapsulated value to another value.
         *
         * @param value the new value of the encapsulated value
         */
        public void setValue(L value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object object) {
            if (object instanceof Left<?, ?>) {
                final Left<?, ?> other = (Left<?, ?>) object;
                return value.equals(other.value);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        @Override
        public String toString() {
            return "Left(" + value.toString() + ")";
        }

        /** Creates a left value of {@link Either}. */
        public static <L, R> Left<L, R> of(L left) {
            return new Left<L, R>(left);
        }
    }

    /**
     * A right value of {@link Either}.
     *
     * @param <L> the type of Left
     * @param <R> the type of Right
     */
    public static class Right<L, R> extends Either<L, R> {
        private R value;

        private Left<L, R> left;

        public Right(R value) {
            this.value = java.util.Objects.requireNonNull(value);
        }

        @Override
        public L left() {
            throw new IllegalStateException("Cannot retrieve Left value on a Right");
        }

        @Override
        public R right() {
            return value;
        }

        /**
         * Sets the encapsulated value to another value.
         *
         * @param value the new value of the encapsulated value
         */
        public void setValue(R value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object object) {
            if (object instanceof Right<?, ?>) {
                final Right<?, ?> other = (Right<?, ?>) object;
                return value.equals(other.value);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        @Override
        public String toString() {
            return "Right(" + value.toString() + ")";
        }

        /** Creates a right value of {@link Either}. */
        public static <L, R> Right<L, R> of(R right) {
            return new Right<L, R>(right);
        }
    }
}
