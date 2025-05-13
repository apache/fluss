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

import com.alibaba.fluss.types.DataTypeRoot;

/**
 * Use this transform when the source is a date or timestamp. It transforms the date to a different
 * granularity such as a timestamp to a day value.
 */
abstract class TimeTransform<S> implements Transform<S, Integer> {
  protected static <R> R fromDataTypeRoot(
      DataTypeRoot type, R dateResult, R timeResult, R timeStampResult, R localTimeZoneResult) {
    switch (type) {
      case DATE:
        return dateResult;
      case TIME_WITHOUT_TIME_ZONE:
        return timeResult;
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return timeStampResult;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return localTimeZoneResult;
    }
    throw new IllegalArgumentException("No Matching Type");
  }

  @Override
  public boolean canTransform(DataTypeRoot type) {
    switch (type) {
      case DATE:
        return true;
      case TIME_WITHOUT_TIME_ZONE:
        return true;
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return true;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return true;
    }
    return false;
  }
}
