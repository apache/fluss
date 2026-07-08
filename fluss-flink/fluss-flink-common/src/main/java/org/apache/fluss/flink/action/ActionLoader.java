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

package org.apache.fluss.flink.action;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.flink.adapter.MultipleParameterToolAdapter;

import java.util.Arrays;
import java.util.Optional;
import java.util.ServiceLoader;

/**
 * Discovers {@link ActionFactory} implementations via {@link ServiceLoader} and dispatches CLI
 * arguments to the appropriate {@link Action}.
 */
@Internal
public final class ActionLoader {

    private ActionLoader() {}

    /**
     * Resolve and create an action from CLI arguments.
     *
     * <p>Returns {@link Optional#empty()} when no arguments are provided or when {@code --help} is
     * requested. Throws {@link IllegalArgumentException} when the requested identifier does not
     * resolve to a known factory.
     */
    public static Optional<Action> createAction(String[] args) {
        if (args.length < 1) {
            printDefaultHelp();
            return Optional.empty();
        }
        if (isHelp(args[0])) {
            printDefaultHelp();
            return Optional.empty();
        }
        String name = args[0].toLowerCase().replace('-', '_');
        ActionFactory factory =
                findFactory(name)
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Unknown action: "
                                                        + args[0]
                                                        + ". Run with --help for available actions."));
        String[] remaining = Arrays.copyOfRange(args, 1, args.length);
        if (hasHelp(remaining)) {
            System.out.println(factory.help());
            return Optional.empty();
        }
        MultipleParameterToolAdapter params = MultipleParameterToolAdapter.fromArgs(remaining);
        return factory.create(params);
    }

    private static boolean isHelp(String arg) {
        return "--help".equals(arg) || "-h".equals(arg);
    }

    private static boolean hasHelp(String[] args) {
        for (String arg : args) {
            if (isHelp(arg)) {
                return true;
            }
        }
        return false;
    }

    private static Optional<ActionFactory> findFactory(String identifier) {
        for (ActionFactory f : ServiceLoader.load(ActionFactory.class)) {
            if (f.identifier().equals(identifier)) {
                return Optional.of(f);
            }
        }
        return Optional.empty();
    }

    private static void printDefaultHelp() {
        System.out.println("Usage: <action-name> [options]");
        System.out.println("Available actions:");
        for (ActionFactory f : ServiceLoader.load(ActionFactory.class)) {
            System.out.println("  " + f.identifier());
        }
    }
}
