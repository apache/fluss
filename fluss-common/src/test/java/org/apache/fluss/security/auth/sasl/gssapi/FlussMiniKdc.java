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

package org.apache.fluss.security.auth.sasl.gssapi;

import org.apache.hadoop.minikdc.MiniKdc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.UUID;

/** A wrapper around {@link MiniKdc} for running Kerberos KDC in tests. */
public class FlussMiniKdc {
    private static final Logger LOG = LoggerFactory.getLogger(FlussMiniKdc.class);
    private final File workDir;
    private final Properties conf;
    private MiniKdc kdc;

    public FlussMiniKdc(Properties conf) throws Exception {
        this.conf = conf;
        // Force binding to 127.0.0.1 to avoid connection refused on dual-stack systems
        this.conf.setProperty(MiniKdc.KDC_BIND_ADDRESS, "127.0.0.1");
        Path tempDir = Files.createTempDirectory("fluss-kdc-" + UUID.randomUUID());
        this.workDir = tempDir.toFile();
    }

    public void start() throws Exception {
        if (kdc != null) {
            return;
        }
        kdc = new MiniKdc(conf, workDir);
        kdc.start();
        LOG.info("MiniKdc started at {}:{}", kdc.getHost(), kdc.getPort());
    }

    public void stop() {
        if (kdc != null) {
            kdc.stop();
            kdc = null;
        }
        // Basic cleanup; in real tests, use JUnit's @TempDir or similar for better cleanup
        deleteDir(workDir);
    }

    public void createPrincipal(File keytab, String... principals) throws Exception {
        kdc.createPrincipal(keytab, principals);
    }

    public File getKrb5Conf() {
        // MiniKdc usually creates krb5.conf in the work directory
        File krb5Conf = new File(workDir, "krb5.conf");
        if (krb5Conf.exists()) {
            return krb5Conf;
        }

        // If not found, search subdirectories (MiniKdc might create timestamped dirs)
        File[] files = workDir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    File nestedConf = new File(file, "krb5.conf");
                    if (nestedConf.exists()) {
                        return nestedConf;
                    }
                }
            }
        }

        // Return default path even if not found
        return krb5Conf;
    }

    public String getRealm() {
        return kdc.getRealm();
    }

    public int getPort() {
        return kdc.getPort();
    }

    private void deleteDir(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File f : files) {
                    deleteDir(f);
                }
            }
        }
        file.delete();
    }
}
