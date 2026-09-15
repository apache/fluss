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

package com.alibaba.fluss.cli;

import com.alibaba.fluss.cli.cmd.base.BaseParentCmd;
import com.alibaba.fluss.cli.format.CliStrings;
import com.alibaba.fluss.cli.format.CommanderFactory;
import com.alibaba.fluss.cli.utils.CmdUtils;
import com.alibaba.fluss.cli.utils.ConnectionUtils;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.GlobalConfiguration;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/** Main command entrance. */
@Parameters(
        commandNames = FlussCliMain.MAIN_CMD,
        commandDescription = "Fluss Distributed Stream Processing Platform CLI")
public class FlussCliMain {

    // Global Parameters
    @Parameter(
            names = {"--bootstrap-server", "-b"},
            description = "Cluster bootstrap servers (comma-separated)")
    private String bootstrapServers;

    @Parameter(
            names = {"--config", "-c"},
            description = "Path to configuration file")
    private String configFile;

    @Parameter(
            names = {"--version", "-v"},
            description = "Show version info",
            help = true)
    private boolean version;

    @Parameter(
            names = {"--help", "-h"},
            description = "Show usage help",
            help = true)
    private boolean help;

    public static final String MAIN_CMD = "fluss";

    private final JCommander jc;

    private final Supplier<Admin> adminSupplier = () -> ConnectionUtils.getAdmin(bootstrapServers);

    private static final List<String> keywords = new ArrayList<>();

    public static void main(String[] args) {
        FlussCliMain cli = new FlussCliMain();
        int exitCode = cli.run(args);
        System.exit(exitCode);
    }

    public FlussCliMain() {
        jc = CommanderFactory.createCommander(MAIN_CMD, this);
        // Register commands
        CmdUtils.registerCommand(
                adminSupplier,
                jc,
                BaseParentCmd.class::isAssignableFrom,
                c -> {
                    Parameters param = c.getAnnotation(Parameters.class);
                    keywords.addAll(Arrays.asList(param.commandNames()));
                });
    }

    private static void printBanner() {
        CliStrings.print();
    }

    public int run(String[] args) {
        try {
            List<String[]> splitArgs = splitByKeywords(args);
            jc.parse(splitArgs.get(0));

            if (version) {
                CliStrings.printVersion();
                return 0;
            }

            if (help || Objects.isNull(jc.getParsedCommand())) {
                printBanner();
                jc.usage();
                return 0;
            }
            initArgs();
            return dispatchCommand(splitArgs.get(1));

        } catch (Exception e) {
            System.out.println("Main Cli Error: " + e.getMessage());
            jc.usage();
            return 1;
        }
    }

    private void initArgs() {
        if (!Objects.isNull(configFile)) {
            if (!Files.exists(Paths.get(configFile))) {
                throw new ParameterException("Config file not found: " + configFile);
            }
            Configuration config = GlobalConfiguration.loadConfiguration(configFile, null);
            bootstrapServers =
                    String.join(
                            ",",
                            Optional.ofNullable(config.get(ConfigOptions.BOOTSTRAP_SERVERS))
                                    .orElseThrow(
                                            () ->
                                                    new ParameterException(
                                                            "Missing bootstrap servers")));
        }

        List<String> servers = Arrays.asList(bootstrapServers.split(","));
        servers.forEach(
                server -> {
                    if (!server.matches("^\\S+:\\d{1,5}$")) {
                        throw new ParameterException(
                                "Invalid server format: " + server + " (expected host:port)");
                    }
                });
    }

    private static List<String[]> splitByKeywords(String[] args) {
        OptionalInt keywordIndex =
                IntStream.range(0, args.length).filter(i -> keywords.contains(args[i])).findFirst();
        if (!keywordIndex.isPresent()) {
            return Arrays.asList(args.clone(), new String[0]);
        }

        int splitPos = keywordIndex.getAsInt();
        return Arrays.asList(
                Arrays.copyOfRange(args, 0, splitPos + 1), // Main cmd and args
                Arrays.copyOfRange(args, splitPos + 1, args.length)); // Sub command args.
    }

    private int dispatchCommand(String[] subArgs) throws Exception {
        String command = jc.getParsedCommand();
        List<Object> cmds = jc.getCommands().get(command).getObjects();
        BaseParentCmd baseGroupCmd = (BaseParentCmd) cmds.get(0);
        baseGroupCmd.setArgs(subArgs);
        return baseGroupCmd.execute();
    }
}
