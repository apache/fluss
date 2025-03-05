/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.connector.flink.utils;

import com.alibaba.fluss.exception.DatabaseAlreadyExistException;
import com.alibaba.fluss.exception.DatabaseNotEmptyException;
import com.alibaba.fluss.exception.DatabaseNotExistException;
import com.alibaba.fluss.exception.InvalidPartitionException;
import com.alibaba.fluss.exception.PartitionAlreadyExistsException;
import com.alibaba.fluss.exception.PartitionNotExistException;
import com.alibaba.fluss.exception.TableAlreadyExistException;
import com.alibaba.fluss.exception.TableNotExistException;
import com.alibaba.fluss.exception.TableNotPartitionedException;

/** Utility class for catalog exceptions. */
public class CatalogExceptionUtils {

    private CatalogExceptionUtils() {}

    public static boolean isDatabaseNotExist(Throwable throwable) {
        return throwable instanceof DatabaseNotExistException;
    }

    public static boolean isDatabaseNotEmpty(Throwable throwable) {
        return throwable instanceof DatabaseNotEmptyException;
    }

    public static boolean isDatabaseAlreadyExist(Throwable throwable) {
        return throwable instanceof DatabaseAlreadyExistException;
    }

    public static boolean isTableNotExist(Throwable throwable) {
        return throwable instanceof TableNotExistException;
    }

    public static boolean isTableAlreadyExist(Throwable throwable) {
        return throwable instanceof TableAlreadyExistException;
    }

    public static boolean isTableNotPartitioned(Throwable throwable) {
        return throwable instanceof TableNotPartitionedException;
    }

    public static boolean isPartitionAlreadyExists(Throwable throwable) {
        return throwable instanceof PartitionAlreadyExistsException;
    }

    public static boolean isPartitionNotExist(Throwable throwable) {
        return throwable instanceof PartitionNotExistException;
    }

    public static boolean isPartitionInvalid(Throwable throwable) {
        return throwable instanceof InvalidPartitionException;
    }
}
