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

package org.apache.fluss.fs.azure;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.config.ConfigOption;

import static org.apache.fluss.config.ConfigBuilder.key;

/** Config options for Azure FileSystem. */
@PublicEvolving
public class AzureFileSystemOptions {

    public static final ConfigOption<String> ACCOUNT_KEY =
            key("fs.azure.account.key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Azure storage account key.");

    public static final ConfigOption<String> CLIENT_ID =
            key("fs.azure.account.oauth2.client.id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Azure OAuth2 client ID.");

    public static final ConfigOption<String> CLIENT_SECRET =
            key("fs.azure.account.oauth2.client.secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Azure OAuth2 client secret.");

    public static final ConfigOption<String> ENDPOINT_KEY =
            key("fs.azure.account.oauth2.client.endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Azure OAuth2 client endpoint.");

    public static final ConfigOption<String> PROVIDER_CONFIG_NAME =
            key("fs.azure.account.oauth.provider.type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Azure OAuth provider type.");
}
