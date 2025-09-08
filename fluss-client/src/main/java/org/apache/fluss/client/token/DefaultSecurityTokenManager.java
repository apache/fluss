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

package org.apache.fluss.client.token;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.fs.token.ObtainedSecurityToken;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.config.ConfigOptions.FILESYSTEM_SECURITY_TOKEN_RENEWAL_RETRY_BACKOFF;
import static org.apache.fluss.config.ConfigOptions.FILESYSTEM_SECURITY_TOKEN_RENEWAL_TIME_RATIO;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Manager for security tokens to access files in fluss client. */
public class DefaultSecurityTokenManager implements SecurityTokenManager {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultSecurityTokenManager.class);

    private final double tokensRenewalTimeRatio;

    private final long renewalRetryBackoffPeriod;

    private final SecurityTokenReceiverRepository securityTokenReceiverRepository;

    private final SecurityTokenProvider securityTokenProvider;

    private final ScheduledExecutorService scheduledExecutor;

    private final Object tokensUpdateFutureLock = new Object();

    @GuardedBy("tokensUpdateFutureLock")
    @Nullable
    private ScheduledFuture<?> tokensUpdateFuture;

    public DefaultSecurityTokenManager(
            Configuration configuration, SecurityTokenProvider securityTokenProvider) {
        this.securityTokenProvider = securityTokenProvider;
        this.tokensRenewalTimeRatio =
                configuration.get(FILESYSTEM_SECURITY_TOKEN_RENEWAL_TIME_RATIO);
        this.renewalRetryBackoffPeriod =
                configuration.get(FILESYSTEM_SECURITY_TOKEN_RENEWAL_RETRY_BACKOFF).toMillis();

        this.securityTokenReceiverRepository = new SecurityTokenReceiverRepository();

        this.scheduledExecutor =
                Executors.newScheduledThreadPool(
                        1, new ExecutorThreadFactory("periodic-token-renew-scheduler"));
    }

    @Override
    public void start() throws Exception {
        synchronized (tokensUpdateFutureLock) {
            checkState(tokensUpdateFuture == null, "Manager is already started");
        }

        startTokensUpdate();
    }

    void startTokensUpdate() {
        try {
            LOG.info("Starting tokens update task");
            AtomicReference<ObtainedSecurityToken> tokenContainer = new AtomicReference<>();
            Optional<Long> nextRenewal = obtainSecurityTokensAndGetNextRenewal(tokenContainer);

            if (tokenContainer.get() != null) {
                securityTokenReceiverRepository.onNewTokensObtained(tokenContainer.get());
            } else {
                LOG.warn("No tokens obtained so skipping notifications");
            }

            if (nextRenewal.isPresent()) {
                long renewalDelay =
                        calculateRenewalDelay(Clock.systemDefaultZone(), nextRenewal.get());
                synchronized (tokensUpdateFutureLock) {
                    tokensUpdateFuture =
                            scheduledExecutor.schedule(
                                    this::startTokensUpdate, renewalDelay, TimeUnit.MILLISECONDS);
                }
                LOG.info("Tokens update task started with {} ms delay", renewalDelay);
            } else {
                LOG.warn(
                        "Tokens update task not started because either no tokens obtained or none of the tokens specified its renewal date");
            }
        } catch (Exception e) {
            synchronized (tokensUpdateFutureLock) {
                tokensUpdateFuture =
                        scheduledExecutor.schedule(
                                this::startTokensUpdate,
                                renewalRetryBackoffPeriod,
                                TimeUnit.MILLISECONDS);
            }
            LOG.warn(
                    "Failed to update tokens, will try again in {} ms",
                    renewalRetryBackoffPeriod,
                    e);
        }
    }

    protected Optional<Long> obtainSecurityTokensAndGetNextRenewal(
            AtomicReference<ObtainedSecurityToken> tokenContainer) {
        try {
            LOG.debug("Obtaining security token.");
            ObtainedSecurityToken token = securityTokenProvider.obtainSecurityToken();
            tokenContainer.set(token);
            checkNotNull(token, "Obtained security tokens must not be null");
            LOG.debug("Obtained security token successfully");
            return token.getValidUntil();
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            LOG.error("Failed to obtain security token.", t);
            throw new FlussRuntimeException(t);
        }
    }

    @VisibleForTesting
    void stopTokensUpdate() {
        synchronized (tokensUpdateFutureLock) {
            if (tokensUpdateFuture != null) {
                tokensUpdateFuture.cancel(true);
                tokensUpdateFuture = null;
            }
        }
    }

    @VisibleForTesting
    long calculateRenewalDelay(Clock clock, long nextRenewal) {
        long now = clock.millis();
        long renewalDelay = Math.round(tokensRenewalTimeRatio * (nextRenewal - now));
        LOG.debug(
                "Calculated delay on renewal is {}, based on next renewal {} and the ratio {}, and current time {}",
                renewalDelay,
                nextRenewal,
                tokensRenewalTimeRatio,
                now);
        return renewalDelay;
    }

    /** Stops re-occurring token obtain task. */
    @Override
    public void stop() {
        LOG.info("Stopping security token renewal");

        stopTokensUpdate();

        scheduledExecutor.shutdownNow();

        LOG.info("Stopped security token renewal");
    }
}
