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
 * SecurityTokenReceiver for Azure Blob Storage using the WASB (Windows Azure Storage Blob) driver.
 * Registered for the {@code wasb://} scheme.
 *
 * <p>WASB is the legacy driver for accessing Azure Blob Storage. Consider using ABFS for new
 * deployments as it provides better performance and security features.
 *
 * <p>URI format: {@code wasb://<container>@<storage-account>.blob.core.windows.net/<path>}
 */
public class WasbDelegationTokenReceiver extends AzureDelegationTokenReceiver {

    public String scheme() {
        return "wasb";
    }
}
