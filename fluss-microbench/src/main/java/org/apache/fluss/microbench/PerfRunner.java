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

package org.apache.fluss.microbench;

import org.apache.fluss.microbench.config.ScenarioConfig;
import org.apache.fluss.microbench.config.ScenarioValidator;
import org.apache.fluss.microbench.engine.YamlDrivenEngine;
import org.apache.fluss.microbench.report.PerfReport;
import org.apache.fluss.microbench.report.ReportWriter;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/** Command-line entry point for YAML benchmark scenarios. */
public final class PerfRunner {

    private static final String[] PRESETS = {"kv-upsert-get", "kv-agg-mixed", "kv-agg-rbm32"};

    private PerfRunner() {}

    public static void main(String[] args) {
        try {
            System.exit(execute(args));
        } catch (Exception e) {
            System.err.println("Benchmark failed: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    static int execute(String[] args) throws Exception {
        if (args.length == 1 && "list".equals(args[0])) {
            for (String preset : PRESETS) {
                System.out.println(preset);
            }
            return 0;
        }
        boolean validate = args.length == 3 && "validate".equals(args[0]);
        boolean run =
                args.length == 5
                        && "run".equals(args[0])
                        && "--bootstrap-servers".equals(args[3])
                        && !args[4].trim().isEmpty();
        if (!(validate || run) || !"--scenario-file".equals(args[1])) {
            System.err.println(
                    "Usage: fluss-microbench.sh run --scenario-file <file|preset> --bootstrap-servers <host:port>");
            System.err.println("       fluss-microbench.sh validate --scenario-file <file|preset>");
            System.err.println("       fluss-microbench.sh list");
            return 1;
        }
        ScenarioConfig config = PerfConfig.load(args[2]);
        List<String> errors = ScenarioValidator.validate(config);
        for (String error : errors) {
            System.err.println(error);
        }
        if (!errors.isEmpty()) {
            return 1;
        }
        if (validate) {
            System.out.println("Valid scenario: " + args[2]);
            return 0;
        }

        String name = config.meta() != null ? config.meta().name() : args[2];
        String timestamp =
                DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").format(LocalDateTime.now());
        Path runDir = MicrobenchPaths.fromSystemProperty().runDir(name, timestamp);
        PerfReport report = new YamlDrivenEngine().run(config, args[4]);
        ReportWriter.write(report, runDir);
        System.out.println("Result: " + runDir.resolve("summary.json"));
        return "complete".equals(report.status()) ? 0 : 1;
    }
}
