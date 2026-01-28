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

package org.apache.fluss.metrics.prometheus;

import org.apache.fluss.metrics.Metric;

import io.prometheus.client.exporter.HTTPServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;

import static org.apache.fluss.utils.Preconditions.checkState;

/** {@link PrometheusReporter} that exports {@link Metric} via Prometheus HTTP server. */
public class PrometheusReporter extends AbstractPrometheusReporter {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusReporter.class);

    private HTTPServer httpServer;
    private int port;

    int getPort() {
        checkState(httpServer != null, "Server has not been initialized.");
        return port;
    }

    PrometheusReporter(Iterator<Integer> ports) {
        while (ports.hasNext()) {
            port = ports.next();
            try {
                httpServer = new HTTPServer(new InetSocketAddress(port), this.registry);
                LOG.info("Started PrometheusReporter HTTP server on port {}.", port);
                break;
            } catch (IOException ioe) { // assume port conflict
                LOG.debug("Could not start PrometheusReporter HTTP server on port {}.", port, ioe);
            }
        }

        if (httpServer == null) {
            throw new RuntimeException(
                    "Could not start PrometheusReporter HTTP server on any configured port. Ports: "
                            + ports);
        }
    }

    @Override
    public void close() {
        if (httpServer != null) {
            httpServer.stop();
        }
        super.close();
    }
}
