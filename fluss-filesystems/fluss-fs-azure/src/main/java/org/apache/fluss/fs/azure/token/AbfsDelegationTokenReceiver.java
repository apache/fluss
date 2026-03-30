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
 * SecurityTokenReceiver for Azure Blob Storage using the ABFS (Azure Blob File System) driver.
 * Registered for the {@code abfs://} scheme.
 *
 * <p>ABFS is the recommended driver for accessing Azure Data Lake Storage Gen2. Use this scheme for
 * non-SSL connections to ADLS Gen2 storage accounts.
 *
 * <p>URI format: {@code abfs://<container>@<storage-account>.dfs.core.windows.net/<path>}
 */
public class AbfsDelegationTokenReceiver extends AzureDelegationTokenReceiver {

    @Override
    public String scheme() {
        return "abfs";
    }
}
