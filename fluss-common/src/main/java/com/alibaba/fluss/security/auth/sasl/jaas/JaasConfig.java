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

package com.alibaba.fluss.security.auth.sasl.jaas;

import com.alibaba.fluss.exception.FlussRuntimeException;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A JAAS configuration parser that constructs an in-memory {@link Configuration} from a
 * string-based JAAS configuration entry, typically provided via Fluss configuration properties.
 *
 * <p>This class allows specifying JAAS login modules and their options inline (e.g., in
 * configuration files or command-line parameters), without requiring an external `jaas.conf` file.
 * It supports the standard JAAS configuration syntax:
 *
 * <pre>{@code
 * <loginModuleClass> <controlFlag> (<optionName>=<optionValue>)*;
 * }</pre>
 *
 * <p>For example:
 *
 * <pre>{@code
 * com.example.SampleLoginModule required username="user" password="pass";
 * }</pre>
 *
 * <p>This implementation is particularly useful for dynamic environments where configurations are
 * injected at runtime, such as containerized deployments or testing scenarios.
 */
public class JaasConfig extends Configuration {

    private final String loginContextName;
    private final List<AppConfigurationEntry> configEntries;

    public JaasConfig(String loginContextName, String jaasConfigParams) {
        StreamTokenizer tokenizer = new StreamTokenizer(new StringReader(jaasConfigParams));
        // allow comments with "/* */" and "//".
        tokenizer.slashSlashComments(true);
        tokenizer.slashStarComments(true);
        // seen "-", "-" and "$" as word characters rather than delimiters.
        tokenizer.wordChars('-', '-');
        tokenizer.wordChars('_', '_');
        tokenizer.wordChars('$', '$');

        try {
            configEntries = new ArrayList<>();
            while (tokenizer.nextToken() != StreamTokenizer.TT_EOF) {
                configEntries.add(parseAppConfigurationEntry(tokenizer));
            }
            if (configEntries.isEmpty()) {
                throw new IllegalArgumentException("Login module not specified in JAAS config");
            }

            this.loginContextName = loginContextName;

        } catch (IOException e) {
            throw new FlussRuntimeException("Unexpected exception while parsing JAAS config");
        }
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        if (this.loginContextName.equals(name)) {
            return configEntries.toArray(new AppConfigurationEntry[0]);
        } else {
            return null;
        }
    }

    private AppConfigurationEntry.LoginModuleControlFlag loginModuleControlFlag(String flag) {
        if (flag == null) {
            throw new IllegalArgumentException(
                    "Login module control flag is not available in the JAAS config");
        }

        AppConfigurationEntry.LoginModuleControlFlag controlFlag;
        switch (flag.toUpperCase(Locale.ROOT)) {
            case "REQUIRED":
                controlFlag = AppConfigurationEntry.LoginModuleControlFlag.REQUIRED;
                break;
            case "REQUISITE":
                controlFlag = AppConfigurationEntry.LoginModuleControlFlag.REQUISITE;
                break;
            case "SUFFICIENT":
                controlFlag = AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT;
                break;
            case "OPTIONAL":
                controlFlag = AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL;
                break;
            default:
                throw new IllegalArgumentException(
                        "Invalid jaas module control flag '" + flag + "' in JAAS config");
        }
        return controlFlag;
    }

    private AppConfigurationEntry parseAppConfigurationEntry(StreamTokenizer tokenizer)
            throws IOException {
        String loginModule = tokenizer.sval;
        if (tokenizer.nextToken() == StreamTokenizer.TT_EOF) {
            throw new IllegalArgumentException(
                    "Login module control flag not specified in JAAS config");
        }
        AppConfigurationEntry.LoginModuleControlFlag controlFlag =
                loginModuleControlFlag(tokenizer.sval);
        Map<String, String> options = new HashMap<>();
        while (tokenizer.nextToken() != StreamTokenizer.TT_EOF && tokenizer.ttype != ';') {
            String key = tokenizer.sval;
            if (tokenizer.nextToken() != '='
                    || tokenizer.nextToken() == StreamTokenizer.TT_EOF
                    || tokenizer.sval == null) {
                throw new IllegalArgumentException(
                        "Value not specified for key '" + key + "' in JAAS config");
            }
            String value = tokenizer.sval;
            options.put(key, value);
        }
        if (tokenizer.ttype != ';') {
            throw new IllegalArgumentException("JAAS config entry not terminated by semi-colon");
        }
        return new AppConfigurationEntry(loginModule, controlFlag, options);
    }
}
