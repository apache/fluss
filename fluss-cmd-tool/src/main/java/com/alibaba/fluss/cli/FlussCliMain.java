/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.cli;

import com.alibaba.fluss.client.admin.Admin;

import org.reflections.Reflections;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Help.Ansi.Style;
import picocli.CommandLine.Help.Visibility;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ScopeType;
import picocli.CommandLine.Spec;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Fluss cmd cli main. */
@Command(
        name = "fluss",
        scope = ScopeType.INHERIT,
        mixinStandardHelpOptions = true,
        usageHelpWidth = 120,
        description = "Fluss Command Line Interface",
        subcommands = {CommandLine.HelpCommand.class})
public class FlussCliMain implements Callable<Integer> {

    @Spec CommandSpec spec;

    @ArgGroup(exclusive = false, multiplicity = "1")
    private final ClusterOptions opts = new ClusterOptions();

    static class ClusterOptions {
        @CommandLine.Option(
                names = {"-c", "--config"},
                description = "Config file path",
                defaultValue = "FLUSS_HOME/conf/fluss.conf",
                showDefaultValue = Visibility.ALWAYS)
        public String configPath;

        @CommandLine.Option(
                names = {"-b", "--bootstrap-servers"},
                split = ",",
                arity = "1..*",
                description =
                        "Host:port pairs for cluster connection (format: host1:port1,host2:port2)",
                paramLabel = "HOST:PORT")
        protected List<String> bootstrapServers;
    }

    final Supplier<Admin> adminSupplier = () -> ConnectionUtils.getAdmin(opts.bootstrapServers);

    private static final String CMD_PKG = "com.alibaba.fluss.cli";

    public static void main(String[] args) {
        FlussCliMain cli = new FlussCliMain();

        Help.ColorScheme schema =
                new Help.ColorScheme.Builder()
                        .commands(Style.bold, Style.underline)
                        .options(Style.fg_yellow)
                        .errors(Style.fg_red, Style.bold)
                        .ansi(Ansi.ON)
                        .build();

        CommandLine cmd = new CommandLine(cli);
        cmd.setColorScheme(schema);
        cmd.setUsageHelpAutoWidth(true);

        cli.registerSubcommands(cmd);

        System.exit(cmd.execute(args));
    }

    @Override
    public Integer call() {
        spec.commandLine().usage(System.out);
        return 0;
    }

    private void registerSubcommands(CommandLine root) {
        Reflections reflections = new Reflections(CMD_PKG);
        Set<Class<?>> all = reflections.getTypesAnnotatedWith(Command.class);

        Set<Class<?>> subCommands =
                all.stream()
                        .filter(cls -> !cls.equals(FlussCliMain.class))
                        .filter(cls -> cls.getEnclosingClass() == null)
                        .collect(Collectors.toCollection(LinkedHashSet::new));

        for (Class<?> cls : subCommands) {
            // fluss is a parent, and the following cmd is the subParent. e.g. Table.
            CommandLine subParent = buildCommandLine(cls);
            subParent.setColorScheme(root.getColorScheme());
            String name = cls.getAnnotation(Command.class).name();
            root.addSubcommand(name, subParent);
            registerNested(subParent, cls);
        }
    }

    private CommandLine buildCommandLine(Class<?> type) {
        try {
            Object obj = type.getDeclaredConstructor().newInstance();
            if (obj instanceof CmdBase) {
                CmdBase cmd = (CmdBase) obj;
                cmd.setFlussAdminSupplier(adminSupplier);
            }
            return new CommandLine(obj);
        } catch (Exception e) {
            throw new RuntimeException("Error instantiating command " + type, e);
        }
    }

    private void attachSubcommand(CommandLine parentCmd, Class<?> childClass) {
        CommandLine childCmd = buildCommandLine(childClass);
        String name = childClass.getAnnotation(Command.class).name();
        parentCmd.addSubcommand(name, childCmd);
        registerNested(childCmd, childClass);
    }

    private void registerNested(CommandLine parentCmd, Class<?> parentClass) {
        for (Class<?> nested : parentClass.getDeclaredClasses()) {
            if (nested.isAnnotationPresent(Command.class)) {
                attachSubcommand(parentCmd, nested);
            }
        }
    }
}
