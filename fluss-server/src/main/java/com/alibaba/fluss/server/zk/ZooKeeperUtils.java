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

package com.alibaba.fluss.server.zk;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.server.utils.FatalErrorHandler;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.framework.CuratorFrameworkFactory;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.framework.api.UnhandledErrorListener;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.framework.state.SessionConnectionStateErrorPolicy;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.retry.ExponentialBackoffRetry;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.stream.Collectors;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/** Class containing helper functions to interact with ZooKeeper. */
public class ZooKeeperUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperUtils.class);

    /**
     * Starts a {@link ZooKeeperClient} instance and connects it to the given ZooKeeper quorum.
     *
     * @param configuration {@link Configuration} object containing the configuration values
     * @param fatalErrorHandler {@link FatalErrorHandler} fatalErrorHandler to handle unexpected
     *     errors of {@link CuratorFramework}
     * @return {@link ZooKeeperClient} instance
     */
    public static ZooKeeperClient startZookeeperClient(
            Configuration configuration, FatalErrorHandler fatalErrorHandler) {
        checkNotNull(configuration, "configuration");
        String zkQuorum = configuration.getValue(ConfigOptions.ZOOKEEPER_ADDRESS);

        if (zkQuorum == null || StringUtils.isBlank(zkQuorum)) {
            throw new RuntimeException(
                    "No valid ZooKeeper quorum has been specified. "
                            + "You can specify the quorum via the configuration key '"
                            + ConfigOptions.ZOOKEEPER_ADDRESS.key()
                            + "'.");
        }

        int sessionTimeout =
                Math.toIntExact(
                        configuration.get(ConfigOptions.ZOOKEEPER_SESSION_TIMEOUT).toMillis());

        int connectionTimeout =
                Math.toIntExact(
                        configuration.get(ConfigOptions.ZOOKEEPER_CONNECTION_TIMEOUT).toMillis());

        int retryWait =
                Math.toIntExact(configuration.get(ConfigOptions.ZOOKEEPER_RETRY_WAIT).toMillis());

        int maxRetryAttempts = configuration.getInt(ConfigOptions.ZOOKEEPER_MAX_RETRY_ATTEMPTS);

        String root = generateZookeeperPath(configuration.getValue(ConfigOptions.ZOOKEEPER_ROOT));

        LOG.info("Using '{}' as Zookeeper root path to stores its entries.", root);

        boolean ensembleTracking =
                configuration.getBoolean(ConfigOptions.ZOOKEEPER_ENSEMBLE_TRACKING);

        final CuratorFrameworkFactory.Builder curatorFrameworkBuilder =
                CuratorFrameworkFactory.builder()
                        .connectString(zkQuorum)
                        .sessionTimeoutMs(sessionTimeout)
                        .connectionTimeoutMs(connectionTimeout)
                        .retryPolicy(new ExponentialBackoffRetry(retryWait, maxRetryAttempts))
                        // Curator prepends a '/' manually and throws an Exception if the
                        // namespace starts with a '/'.
                        .namespace(trimStartingSlash(root))
                        .ensembleTracker(ensembleTracking);

        if (configuration.get(ConfigOptions.ZOOKEEPER_TOLERATE_SUSPENDED_CONNECTIONS)) {
            curatorFrameworkBuilder.connectionStateErrorPolicy(
                    new SessionConnectionStateErrorPolicy());
        }
        return new ZooKeeperClient(
                startZookeeperClient(curatorFrameworkBuilder, fatalErrorHandler));
    }

    /**
     * Starts a {@link CuratorFramework} instance and connects it to the given ZooKeeper quorum from
     * a builder.
     *
     * @param builder {@link CuratorFrameworkFactory.Builder} A builder for curatorFramework.
     * @param fatalErrorHandler {@link FatalErrorHandler} fatalErrorHandler to handle unexpected
     *     errors of {@link CuratorFramework}
     * @return {@link CuratorFrameworkWithUnhandledErrorListener} instance
     */
    @VisibleForTesting
    public static CuratorFrameworkWithUnhandledErrorListener startZookeeperClient(
            CuratorFrameworkFactory.Builder builder, FatalErrorHandler fatalErrorHandler) {
        CuratorFramework cf = builder.build();
        UnhandledErrorListener unhandledErrorListener =
                (message, throwable) -> {
                    LOG.error(
                            "Unhandled error in curator framework, error message: {}",
                            message,
                            throwable);
                    // The exception thrown in UnhandledErrorListener will be caught by
                    // CuratorFramework. So we mostly trigger exit process or interact with main
                    // thread to inform the failure in FatalErrorHandler.
                    fatalErrorHandler.onFatalError(throwable);
                };
        cf.getUnhandledErrorListenable().addListener(unhandledErrorListener);
        cf.start();
        return new CuratorFrameworkWithUnhandledErrorListener(cf, unhandledErrorListener);
    }

    /** Creates a ZooKeeper path of the form "/a/b/.../z". */
    public static String generateZookeeperPath(String... paths) {
        return Arrays.stream(paths)
                .map(ZooKeeperUtils::trimSlashes)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.joining("/", "/", ""));
    }

    public static String trimStartingSlash(String path) {
        return path.startsWith("/") ? path.substring(1) : path;
    }

    private static String trimSlashes(String input) {
        int left = 0;
        int right = input.length() - 1;

        while (left <= right && input.charAt(left) == '/') {
            left++;
        }

        while (right >= left && input.charAt(right) == '/') {
            right--;
        }

        if (left <= right) {
            return input.substring(left, right + 1);
        } else {
            return "";
        }
    }
}
