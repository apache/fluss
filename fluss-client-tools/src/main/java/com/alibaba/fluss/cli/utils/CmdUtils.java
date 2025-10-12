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

package com.alibaba.fluss.cli.utils;

import com.alibaba.fluss.cli.cmd.base.BaseCmd;
import com.alibaba.fluss.cli.format.CmdUsageFormat;
import com.alibaba.fluss.client.admin.Admin;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.reflections.Reflections;

/** Utils for automatically register command. */
public class CmdUtils {

    public static void registerCommand(
            Supplier<Admin> adminSupplier, JCommander cmdJc, Predicate<Class<?>> cmdPredicate) {
        registerCommand(adminSupplier, cmdJc, cmdPredicate, c -> {});
    }

    public static void registerCommand(
            Supplier<Admin> adminSupplier,
            JCommander cmdJc,
            Predicate<Class<?>> cmdPredicate,
            Consumer<Class<?>> preHandle) {
        Reflections reflections = new Reflections("com.alibaba.fluss.cli");
        List<Class<?>> commands =
                new ArrayList<>(reflections.getTypesAnnotatedWith(Parameters.class));
        commands.sort(Comparator.comparing(Class::getName));
        commands.stream()
                .peek(preHandle)
                .filter(cmdPredicate)
                .forEach(
                        cmd -> {
                            try {
                                BaseCmd commandInstance =
                                        (BaseCmd) cmd.getDeclaredConstructor().newInstance();
                                commandInstance.init(adminSupplier);
                                Arrays.stream(cmd.getAnnotation(Parameters.class).commandNames())
                                        .forEach(
                                                commandName -> {
                                                    cmdJc.addCommand(commandName, commandInstance);
                                                    JCommander jc =
                                                            cmdJc.findCommandByAlias(commandName);
                                                    jc.setUsageFormatter(new CmdUsageFormat(jc));
                                                });
                            } catch (Exception e) {
                                System.err.println(
                                        "Initialize sub command failed: " + e.getMessage());
                                throw new RuntimeException(e);
                            }
                        });
    }
}
