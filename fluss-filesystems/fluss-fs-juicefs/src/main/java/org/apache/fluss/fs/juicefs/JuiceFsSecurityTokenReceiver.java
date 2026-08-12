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

package org.apache.fluss.fs.juicefs;

import org.apache.fluss.fs.token.ObtainedSecurityToken;
import org.apache.fluss.fs.token.SecurityTokenReceiver;

/**
 * Security token receiver for JuiceFS filesystems.
 *
 * <p>Unlike the OSS / S3 / COS plugins, Fluss does not perform any STS or delegation-token exchange
 * for JuiceFS: the JuiceFS client itself authenticates locally against the metadata engine and the
 * backing object storage. Consequently {@link
 * org.apache.fluss.fs.juicefs.JuiceFsFileSystem#obtainSecurityToken()} returns an empty placeholder
 * token with scheme {@code "jfs"}.
 *
 * <p>This receiver exists solely to satisfy the contract of {@code SecurityTokenReceiverRepository}
 * on the client side. Without it, every placeholder token arriving with scheme {@code "jfs"} would
 * be reported as {@code "Token arrived for service but no receiver found for it: jfs"}, which is
 * caught inside {@code DefaultSecurityTokenManager} and translated into a periodic re-schedule of
 * the token renewal task after {@code client.filesystem.security.token.renewal.backoff} — a
 * persistent, unnecessary retry / log-noise loop on every JuiceFS-enabled client.
 *
 * <p>The implementation mirrors {@link org.apache.fluss.fs.hdfs.HdfsSecurityTokenReceiver} and is
 * intentionally a no-op.
 */
public class JuiceFsSecurityTokenReceiver implements SecurityTokenReceiver {

    @Override
    public String scheme() {
        return JuiceFsPlugin.SCHEME;
    }

    @Override
    public void onNewTokensObtained(ObtainedSecurityToken token) {
        // no-op: JuiceFS authenticates locally on each node, so there is nothing to install.
    }
}
