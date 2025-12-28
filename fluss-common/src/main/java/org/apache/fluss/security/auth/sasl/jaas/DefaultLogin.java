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

package org.apache.fluss.security.auth.sasl.jaas;

import org.apache.fluss.utils.TemporaryClassLoaderContext;
import org.apache.fluss.utils.concurrent.ShutdownableThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.security.auth.Subject;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import java.util.Random;
import java.util.Set;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** DefaultLogin is a default implementation of {@link Login}. */
public class DefaultLogin implements Login {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultLogin.class);
    // Refresh the ticket 80% of the way through its lifetime
    private static final double TICKET_RENEW_WINDOW_FACTOR = 0.80;
    private static final double TICKET_RENEW_JITTER = 0.05;
    private static final long MIN_RENEWAL_INTERVAL_MS = 60 * 1000L;
    private static final Random RNG = new Random();

    private String contextName;
    private @Nullable javax.security.auth.login.Configuration jaasConfig;
    private LoginContext loginContext;
    private KerberosRefreshThread refreshThread;

    @Override
    public void configure(String contextName, javax.security.auth.login.Configuration jaasConfig) {
        this.contextName = contextName;
        this.jaasConfig = jaasConfig;
    }

    @Override
    public LoginContext login() throws LoginException {
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(DefaultLogin.class.getClassLoader())) {
            loginContext =
                    new LoginContext(
                            contextName,
                            null,
                            callbacks -> {
                                // Nothing here until we support some mechanisms such as sasl/GSSAPI
                                // later.
                                throw new UnsupportedCallbackException(
                                        callbacks[0], "Unrecognized SASL mechanism.");
                            },
                            jaasConfig);
            loginContext.login();
        } catch (LoginException e) {
            LOG.error("Failed to login: ", e);
            throw e;
        }
        LOG.info("Successfully logged in.");
        startRefreshThread();
        return loginContext;
    }

    @Override
    public Subject subject() {
        return loginContext.getSubject();
    }

    @Override
    public String serviceName() {
        if (loginContext != null && loginContext.getSubject() != null) {
            Set<KerberosPrincipal> principals =
                    loginContext.getSubject().getPrincipals(KerberosPrincipal.class);
            if (!principals.isEmpty()) {
                KerberosPrincipal principal = principals.iterator().next();
                String name = principal.getName();
                int slash = name.indexOf('/');
                if (slash > 0) {
                    return name.substring(0, slash);
                }
                int at = name.indexOf('@');
                if (at > 0) {
                    return name.substring(0, at);
                }
                return name;
            }
        }
        return contextName;
    }

    @Override
    public void close() {
        if (refreshThread != null) {
            try {
                refreshThread.shutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            refreshThread = null;
        }
    }

    protected double getTicketRenewWindowFactor() {
        return TICKET_RENEW_WINDOW_FACTOR;
    }

    protected double getTicketRenewJitter() {
        return TICKET_RENEW_JITTER;
    }

    protected long getMinTimeBeforeRelogin() {
        return MIN_RENEWAL_INTERVAL_MS;
    }

    private void startRefreshThread() {
        if (refreshThread != null) {
            return;
        }
        // Only start the thread if we can find a TGT.
        // If there is no TGT, it might be a non-Kerberos login (e.g. PLAIN), so we don't need to
        // refresh.
        KerberosTicket tgt = getTgt(subject());
        if (tgt == null) {
            return;
        }

        refreshThread = new KerberosRefreshThread("fluss-kerberos-refresh-thread-" + contextName);
        refreshThread.start();
    }

    private long getRefreshTime(KerberosTicket tgt) {
        long start = tgt.getStartTime().getTime();
        long expires = tgt.getEndTime().getTime();
        LOG.info("TGT expires: {}", tgt.getEndTime());

        long proposedRefresh =
                start
                        + (long)
                                ((expires - start)
                                        * (getTicketRenewWindowFactor()
                                                + (getTicketRenewJitter() * RNG.nextDouble())));

        if (proposedRefresh > expires) {
            return System.currentTimeMillis();
        }
        return proposedRefresh;
    }

    private KerberosTicket getTgt(Subject subject) {
        Set<KerberosTicket> tickets = subject.getPrivateCredentials(KerberosTicket.class);
        for (KerberosTicket ticket : tickets) {
            if (isTgt(ticket)) {
                return ticket;
            }
        }
        return null;
    }

    private boolean isTgt(KerberosTicket ticket) {
        return ticket.getServer().getName().startsWith("krbtgt/");
    }

    protected void onRenewComplete() {
        // Override for testing
    }

    protected void onRenewFailure(Exception e) {
        // Override for testing
    }

    /**
     * A daemon thread based on {@link ShutdownableThread} that periodically refreshes the Kerberos
     * TGT before it expires to ensure uninterrupted authentication.
     */
    private class KerberosRefreshThread extends ShutdownableThread {
        public KerberosRefreshThread(String name) {
            super(name, true);
            setDaemon(true);
        }

        @Override
        public void doWork() throws Exception {
            KerberosTicket currentTgt = getTgt(subject());
            if (currentTgt == null) {
                LOG.warn("No TGT found. Stopping renewal thread.");
                onRenewFailure(new LoginException("No TGT found"));
                initiateShutdown();
                return;
            }

            long now = System.currentTimeMillis();
            long nextRefresh = getRefreshTime(currentTgt);
            long sleepTime = nextRefresh - now;

            // Use protected method for testing
            sleepTime = Math.max(sleepTime, getMinTimeBeforeRelogin());

            LOG.info("Scheduling Kerberos ticket renewal in {} ms", sleepTime);
            Thread.sleep(sleepTime);

            LOG.info("Renewing Kerberos ticket...");
            try {
                // Skip logout() to avoid authentication gaps for concurrent requests during
                // renewal.
                loginContext.login();
                LOG.info("Kerberos ticket renewed successfully.");
                onRenewComplete();
            } catch (LoginException le) {
                LOG.error("Failed to renew Kerberos ticket. Will retry...", le);
                onRenewFailure(le);
                // Sleep a bit before retrying to avoid tight loop on persistent
                // failure
                Thread.sleep(getMinTimeBeforeRelogin());
            }
        }
    }
}
