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

package org.apache.fluss.flink.procedure;

import org.apache.fluss.client.admin.Admin;

import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.procedures.Procedure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** ProcedureUtil to load procedure. */
public class ProcedureManager {

    private static final Logger LOG = LoggerFactory.getLogger(ProcedureManager.class);
    private static final Map<String, Class<? extends ProcedureBase>> PROCEDURE_MAP =
            initProcedureMap();

    public static List<String> listProcedures() {
        return new ArrayList<>(PROCEDURE_MAP.keySet());
    }

    public static Optional<Procedure> getProcedure(Admin admin, ObjectPath procedurePath) {
        try {
            Class<? extends ProcedureBase> procedureClass =
                    PROCEDURE_MAP.get(procedurePath.getFullName().toLowerCase());
            if (procedureClass == null) {
                return Optional.empty();
            }
            ProcedureBase instance = procedureClass.getDeclaredConstructor().newInstance();
            instance.withAdmin(admin);
            return Optional.of(instance);
        } catch (Exception e) {
            LOG.error("Failed to instantiate procedure: {}", procedurePath, e);
            return Optional.empty();
        }
    }

    private static Map<String, Class<? extends ProcedureBase>> initProcedureMap() {
        Map<String, Class<? extends ProcedureBase>> map = new HashMap<>();
        for (ProcedureEnum proc : ProcedureEnum.values()) {
            map.put(proc.getPath(), proc.getProcedureClass());
        }
        return Collections.unmodifiableMap(map);
    }

    private enum ProcedureEnum {
        ADD_ACL("sys.add_acl", AddAclProcedure.class),
        DROP_ACL("sys.drop_acl", DropAclProcedure.class),
        List_ACL("sys.list_acl", ListAclProcedure.class),
        SET_CLUSTER_CONFIG("sys.set_cluster_config", SetClusterConfigProcedure.class),
        GET_CLUSTER_CONFIG("sys.get_cluster_config", GetClusterConfigProcedure.class),
        ADD_SERVER_TAG("sys.add_server_tag", AddServerTagProcedure.class),
        REMOVE_SERVER_TAG("sys.remove_server_tag", RemoveServerTagProcedure.class);

        private final String path;
        private final Class<? extends ProcedureBase> procedureClass;

        ProcedureEnum(String identifier, Class<? extends ProcedureBase> procedureClass) {
            this.path = identifier;
            this.procedureClass = procedureClass;
        }

        public String getPath() {
            return path;
        }

        public Class<? extends ProcedureBase> getProcedureClass() {
            return procedureClass;
        }
    }
}
