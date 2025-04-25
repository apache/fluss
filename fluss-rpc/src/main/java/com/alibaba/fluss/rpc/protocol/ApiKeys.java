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

package com.alibaba.fluss.rpc.protocol;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.alibaba.fluss.rpc.protocol.ApiKeys.ApiVisibility.PRIVATE;
import static com.alibaba.fluss.rpc.protocol.ApiKeys.ApiVisibility.PUBLIC;

/** Identifiers for all the Fluss wire protocol APIs. */
public enum ApiKeys {
    // reserve 0~999 for kafka protocol compatibility
    API_VERSIONS(1000, 0, 0, PUBLIC),
    CREATE_DATABASE(1001, 0, 0, PUBLIC),
    DROP_DATABASE(1002, 0, 0, PUBLIC),
    LIST_DATABASES(1003, 0, 0, PUBLIC),
    DATABASE_EXISTS(1004, 0, 0, PUBLIC),
    CREATE_TABLE(1005, 0, 0, PUBLIC),
    DROP_TABLE(1006, 0, 0, PUBLIC),
    GET_TABLE_INFO(1007, 0, 0, PUBLIC),
    LIST_TABLES(1008, 0, 0, PUBLIC),
    LIST_PARTITION_INFOS(1009, 0, 0, PUBLIC),
    TABLE_EXISTS(1010, 0, 0, PUBLIC),
    GET_TABLE_SCHEMA(1011, 0, 0, PUBLIC),
    GET_METADATA(1012, 0, 0, PUBLIC),
    UPDATE_METADATA(1013, 0, 0, PRIVATE),
    PRODUCE_LOG(1014, 0, 0, PUBLIC),
    FETCH_LOG(1015, 0, 0, PUBLIC),
    PUT_KV(1016, 0, 0, PUBLIC),
    LOOKUP(1017, 0, 0, PUBLIC),
    NOTIFY_LEADER_AND_ISR(1018, 0, 0, PRIVATE),
    STOP_REPLICA(1019, 0, 0, PRIVATE),
    ADJUST_ISR(1020, 0, 0, PRIVATE),
    LIST_OFFSETS(1021, 0, 0, PUBLIC),
    COMMIT_KV_SNAPSHOT(1022, 0, 0, PRIVATE),
    GET_LATEST_KV_SNAPSHOTS(1023, 0, 0, PUBLIC),
    GET_KV_SNAPSHOT_METADATA(1024, 0, 0, PUBLIC),
    GET_FILESYSTEM_SECURITY_TOKEN(1025, 0, 0, PUBLIC),
    INIT_WRITER(1026, 0, 0, PUBLIC),
    COMMIT_REMOTE_LOG_MANIFEST(1027, 0, 0, PRIVATE),
    NOTIFY_REMOTE_LOG_OFFSETS(1028, 0, 0, PRIVATE),
    NOTIFY_KV_SNAPSHOT_OFFSET(1029, 0, 0, PRIVATE),
    COMMIT_LAKE_TABLE_SNAPSHOT(1030, 0, 0, PRIVATE),
    NOTIFY_LAKE_TABLE_OFFSET(1031, 0, 0, PRIVATE),
    GET_LATEST_LAKE_SNAPSHOT(1032, 0, 0, PUBLIC),
    LIMIT_SCAN(1033, 0, 0, PUBLIC),
    PREFIX_LOOKUP(1034, 0, 0, PUBLIC),
    GET_DATABASE_INFO(1035, 0, 0, PUBLIC),
    CREATE_PARTITION(1036, 0, 0, PUBLIC),
    DROP_PARTITION(1037, 0, 0, PUBLIC),
    AUTHENTICATE(1038, 0, 0, PUBLIC),
    CREATE_ACLS(1039, 0, 0, PUBLIC),
    LIST_ACLS(1040, 0, 0, PUBLIC),
    DROP_ACLS(1041, 0, 0, PUBLIC);

    private static final Map<Integer, ApiKeys> ID_TO_TYPE =
            Arrays.stream(ApiKeys.values())
                    .collect(Collectors.toMap(key -> (int) key.id, Function.identity()));

    /** the permanent and immutable id of an API - this can't change ever. */
    public final short id;

    public final short lowestSupportedVersion;
    public final short highestSupportedVersion;
    public final ApiVisibility visibility;

    ApiKeys(
            int apiKey,
            int lowestSupportedVersion,
            int highestSupportedVersion,
            ApiVisibility visibility) {
        this.id = (short) apiKey;
        this.lowestSupportedVersion = (short) lowestSupportedVersion;
        this.highestSupportedVersion = (short) highestSupportedVersion;
        this.visibility = visibility;
    }

    @Override
    public String toString() {
        return name() + "(" + id + ")";
    }

    public static ApiKeys forId(int id) {
        return ID_TO_TYPE.get(id);
    }

    public static boolean hasId(int id) {
        return ID_TO_TYPE.containsKey(id);
    }

    /** The API visibility describes who can access the API. */
    public enum ApiVisibility {
        // The API is visible to all the clients.
        PUBLIC,
        // The API is only used for the internal communication between servers of Fluss cluster.
        PRIVATE
    }
}
