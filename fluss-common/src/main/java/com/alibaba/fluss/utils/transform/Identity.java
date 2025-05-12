/*
 *  Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.utils.transform;

import com.alibaba.fluss.types.DataTypeRoot;

/**
 * This is the transform representing no operation being performed. It will return the column as is
 * for use as a partition key. It should be the default if your partition key already exist, or if
 * one of the fields of your compound key is already prepared.
 */
class Identity<T> implements Transform<T, T> {
  private static final Identity<?> INSTANCE = new Identity<>();

  @SuppressWarnings("unchecked")
  public static <I> Identity<I> get() {
    return (Identity<I>) INSTANCE;
  }

  public T apply(T t) {
    return t;
  }

  public boolean isIdentity() {
    return true;
  }

  public boolean canTransform(DataTypeRoot type) {
    return true;
  }

  @Override
  public String toString() {
    return "identity";
  }
}
