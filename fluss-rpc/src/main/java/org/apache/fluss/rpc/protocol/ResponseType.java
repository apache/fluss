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

package org.apache.fluss.rpc.protocol;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Expected response message type sent from RPC server to RPC client. */
public enum ResponseType {
    /** The message is a successful response. */
    SUCCESS_RESPONSE(0),

    /** The message indicates a failed response which is related to a specific request. */
    ERROR_RESPONSE(1),

    /** The message indicates a server failure which is not related to a specific request. */
    SERVER_FAILURE(2);

    private static final Map<Integer, ResponseType> ID_TO_TYPE =
            Arrays.stream(ResponseType.values())
                    .collect(Collectors.toMap(type -> (int) type.id, Function.identity()));

    public final byte id;

    ResponseType(int id) {
        this.id = (byte) id;
    }

    public static ResponseType forId(int id) {
        ResponseType responseType = ID_TO_TYPE.get(id);
        if (responseType == null) {
            throw new IllegalStateException("Unexpected response type id '" + id + "'");
        } else {
            return responseType;
        }
    }
}
