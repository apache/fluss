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

package org.apache.fluss.fs.azure.token;

/**
 * SecurityTokenReceiver for Azure Blob Storage using the WASB driver with SSL/TLS encryption.
 * Registered for the {@code wasbs://} scheme.
 *
 * <p>This is the secure (SSL-enabled) variant of WASB for legacy Azure Blob Storage access. For new
 * deployments, consider using {@code abfss://} with Azure Data Lake Storage Gen2.
 *
 * <p>URI format: {@code wasbs://<container>@<storage-account>.blob.core.windows.net/<path>}
 */
public class WasbsDelegationTokenReceiver extends AzureDelegationTokenReceiver {

    @Override
    public String scheme() {
        return "wasbs";
    }
}
