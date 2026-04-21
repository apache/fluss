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

package org.apache.fluss.microbench.config;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.introspector.BeanAccess;
import org.yaml.snakeyaml.introspector.MissingProperty;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Top-level scenario configuration parsed from YAML.
 *
 * <p>Use {@link #parse(String)} to create an instance from a YAML string.
 */
public class ScenarioConfig {

    private MetaConfig meta;
    private ClusterConfig cluster;
    private ClientConfig client;
    private TableConfig table;
    private DataConfig data;
    private List<WorkloadPhaseConfig> workload;
    private SamplingConfig sampling;
    private ReportConfig report;
    private ThresholdsConfig thresholds;

    /** The original YAML source, preserved for inclusion in reports. */
    private String rawYaml;

    /** Warnings collected during parsing (e.g. unrecognized YAML fields). */
    private List<String> parseWarnings = new ArrayList<>();

    /**
     * Parses a YAML string into a {@link ScenarioConfig}.
     *
     * <p>Handles kebab-case YAML keys (e.g. {@code tablet-servers}) by converting them to camelCase
     * Java property names (e.g. {@code tabletServers}). Also post-processes {@link
     * ColumnConfig#agg()} to resolve both simple string and expanded map forms into {@link
     * AggConfig}.
     *
     * <p>Unknown YAML fields are not rejected (for backward compatibility) but are collected into
     * {@link #parseWarnings()} so that validate/run/report can surface them.
     */
    public static ScenarioConfig parse(String yaml) {
        if (yaml == null || yaml.trim().isEmpty()) {
            throw new IllegalArgumentException("Scenario YAML must not be null or empty");
        }

        Constructor constructor = new Constructor(ScenarioConfig.class, new LoaderOptions());
        KebabCasePropertyUtils propertyUtils = new KebabCasePropertyUtils();
        propertyUtils.setSkipMissingProperties(true);
        constructor.setPropertyUtils(propertyUtils);

        Yaml yamlParser = new Yaml(constructor);
        ScenarioConfig config = yamlParser.load(yaml);
        config.rawYaml = yaml;
        config.parseWarnings = new ArrayList<>(propertyUtils.getUnknownFields());

        // Post-process: resolve AggConfig from raw agg values.
        if (config.table() != null && config.table().columns() != null) {
            for (ColumnConfig column : config.table().columns()) {
                column.resolveAgg();
            }
        }

        return config;
    }

    public MetaConfig meta() {
        return meta;
    }

    public void setMeta(MetaConfig meta) {
        this.meta = meta;
    }

    public ClusterConfig cluster() {
        return cluster;
    }

    public void setCluster(ClusterConfig cluster) {
        this.cluster = cluster;
    }

    public ClientConfig client() {
        return client;
    }

    public void setClient(ClientConfig client) {
        this.client = client;
    }

    public TableConfig table() {
        return table;
    }

    public void setTable(TableConfig table) {
        this.table = table;
    }

    public DataConfig data() {
        return data;
    }

    public void setData(DataConfig data) {
        this.data = data;
    }

    public List<WorkloadPhaseConfig> workload() {
        return workload;
    }

    public void setWorkload(List<WorkloadPhaseConfig> workload) {
        this.workload = workload;
    }

    public SamplingConfig sampling() {
        return sampling;
    }

    public void setSampling(SamplingConfig sampling) {
        this.sampling = sampling;
    }

    public ReportConfig report() {
        return report;
    }

    public void setReport(ReportConfig report) {
        this.report = report;
    }

    public ThresholdsConfig thresholds() {
        return thresholds;
    }

    public void setThresholds(ThresholdsConfig thresholds) {
        this.thresholds = thresholds;
    }

    /** Returns the original YAML source string. */
    public String rawYaml() {
        return rawYaml;
    }

    /** Returns warnings collected during YAML parsing (e.g. unrecognized fields). */
    public List<String> parseWarnings() {
        return Collections.unmodifiableList(parseWarnings);
    }

    // -------------------------------------------------------------------------
    //  Kebab-case to camelCase property resolution
    // -------------------------------------------------------------------------

    /**
     * Custom {@link PropertyUtils} that converts kebab-case YAML keys to camelCase Java property
     * names and tracks unrecognized fields.
     */
    private static class KebabCasePropertyUtils extends PropertyUtils {

        private final List<String> unknownFields = new ArrayList<>();

        @Override
        public Property getProperty(Class<? extends Object> type, String name) {
            String camel = kebabToCamel(name);
            Property prop = super.getProperty(type, camel);
            if (prop instanceof MissingProperty) {
                unknownFields.add(type.getSimpleName() + "." + name);
            }
            return prop;
        }

        @Override
        public Property getProperty(
                Class<? extends Object> type, String name, BeanAccess beanAccess) {
            String camel = kebabToCamel(name);
            Property prop = super.getProperty(type, camel, beanAccess);
            if (prop instanceof MissingProperty) {
                unknownFields.add(type.getSimpleName() + "." + name);
            }
            return prop;
        }

        List<String> getUnknownFields() {
            return unknownFields;
        }

        private static String kebabToCamel(String kebab) {
            if (kebab == null || !kebab.contains("-")) {
                return kebab;
            }
            StringBuilder sb = new StringBuilder(kebab.length());
            boolean upper = false;
            for (int i = 0; i < kebab.length(); i++) {
                char c = kebab.charAt(i);
                if (c == '-') {
                    upper = true;
                } else {
                    sb.append(upper ? Character.toUpperCase(c) : c);
                    upper = false;
                }
            }
            return sb.toString();
        }
    }
}
