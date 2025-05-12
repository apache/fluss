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

import com.alibaba.fluss.exception.InvalidPartitionException;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;

import java.time.LocalDateTime;
import java.time.ZoneId;

class Years<T> extends TimeTransform<T> {
  private static final Years<?> INSTANCE = new Years<>();

  @SuppressWarnings("unchecked")
  static <T> Years<T> get() {
    return (Years<T>) INSTANCE;
  }

  @Override
  public boolean isIdentity() {
    return false;
  }

  @Override
  public Integer apply(T t) {
    if (t instanceof TimestampNtz) {
      return ((TimestampNtz) t).toLocalDateTime().getYear();
    } else if (t instanceof TimestampLtz) {
      return LocalDateTime.ofInstant(((TimestampLtz) t).toInstant(), ZoneId.systemDefault())
          .getYear();
    }
    throw new InvalidPartitionException(
        "source column must be date or timestamp for year transform");
  }
}
