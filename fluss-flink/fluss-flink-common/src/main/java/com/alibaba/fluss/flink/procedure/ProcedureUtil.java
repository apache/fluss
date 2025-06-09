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

package com.alibaba.fluss.flink.procedure;

import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.exception.FlussRuntimeException;

import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.procedures.Procedure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/** ProcedureUtil to load procedure. */
public class ProcedureUtil {
    private static final String SYSTEM_DATABASE_NAME = "sys";
    private static final Logger LOG = LoggerFactory.getLogger(ProcedureUtil.class);

    public static List<String> listProcedures() {
        final List<ProcedureBase> discoverProcedures = discoverProcedures();
        return discoverProcedures.stream()
                .map(ProcedureBase::identifier)
                .collect(Collectors.toList());
    }

    public static Optional<Procedure> getProcedure(Admin admin, ObjectPath procedurePath) {
        if (!SYSTEM_DATABASE_NAME.equals(procedurePath.getDatabaseName())) {
            return Optional.empty();
        }
        try {
            ProcedureBase procedureBase = discoverProcedure(procedurePath.getObjectName());
            return Optional.of(procedureBase.withAdmin(admin));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private static ProcedureBase discoverProcedure(String identifier) {
        final List<ProcedureBase> discoverProcedures = discoverProcedures();

        if (discoverProcedures.isEmpty()) {
            throw new FlussRuntimeException(
                    String.format(
                            "Could not find any factories that implement '%s' in the classpath.",
                            ProcedureBase.class.getName()));
        }

        final List<ProcedureBase> matchingProcedures =
                discoverProcedures.stream()
                        .filter(f -> f.identifier().equals(identifier))
                        .collect(Collectors.toList());

        if (matchingProcedures.isEmpty()) {
            throw new FlussRuntimeException(
                    String.format(
                            "Could not find any factory for identifier '%s' that implements '%s' in the classpath.\n\n"
                                    + "Available factory identifiers are:\n\n"
                                    + "%s",
                            identifier,
                            ProcedureBase.class.getName(),
                            discoverProcedures.stream()
                                    .map(ProcedureBase::identifier)
                                    .collect(Collectors.joining("\n"))));
        }
        if (matchingProcedures.size() > 1) {
            throw new FlussRuntimeException(
                    String.format(
                            "Multiple factories for identifier '%s' that implement '%s' found in the classpath.\n\n"
                                    + "Ambiguous factory classes are:\n\n"
                                    + "%s",
                            identifier,
                            ProcedureBase.class.getName(),
                            matchingProcedures.stream()
                                    .map(f -> f.getClass().getName())
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }

        return matchingProcedures.get(0);
    }

    public static List<ProcedureBase> discoverProcedures() {
        final Iterator<ProcedureBase> serviceLoaderIterator =
                ServiceLoader.load(ProcedureBase.class, ProcedureBase.class.getClassLoader())
                        .iterator();

        final List<ProcedureBase> loadResults = new ArrayList<>();
        while (true) {
            try {
                // error handling should also be applied to the hasNext() call because service
                // loading might cause problems here as well
                if (!serviceLoaderIterator.hasNext()) {
                    break;
                }

                loadResults.add(serviceLoaderIterator.next());
            } catch (Throwable t) {
                if (t instanceof NoClassDefFoundError) {
                    LOG.debug(
                            "NoClassDefFoundError when loading a {}. This is expected when trying to load Procedure but no implementation is loaded.",
                            ProcedureBase.class.getCanonicalName(),
                            t);
                } else {
                    throw new RuntimeException(
                            "Unexpected error when trying to load service provider.", t);
                }
            }
        }

        return loadResults;
    }
}
