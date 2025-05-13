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

package com.alibaba.fluss.utils.transform;

/** Factory methods for transforming a source column to its corresponding partition value. */
public class Transforms {

  private Transforms() {}

  public static Transform<?, ?> fromString(String transform) {

    if (transform.equalsIgnoreCase("identity")) {
      return Identity.get();
    } else if (transform.equalsIgnoreCase("year")) {
      return Years.get();
    } else if (transform.equalsIgnoreCase("months")) {
      return Months.get();
    } else if (transform.equalsIgnoreCase("days")) {
      return Days.get();
    } else if (transform.equalsIgnoreCase("hours")) {
      return Hours.get();
    }

    throw new UnsupportedOperationException(String.format("Unsupported transform: %s", transform));
  }

  /** Transforms a column of any type and returns it unchanged. */
  public static <T> Transform<T, T> identity() {
    return Identity.get();
  }

  /** Transforms a column of date or timestamp type and returns the year. */
  public static <T> Transform<T, Integer> years() {
    return Years.get();
  }

  /** Transforms a column of date or timestamp type and returns the month. */
  public static <T> Transform<T, Integer> months() {
    return Months.get();
  }

  /** Transforms a column of date or timestamp type and returns the day. */
  public static <T> Transform<T, Integer> days() {
    return Days.get();
  }

  /** Transforms a column of date or timestamp type and returns the hour. */
  public static <T> Transform<T, Integer> hours() {
    return Hours.get();
  }
}
