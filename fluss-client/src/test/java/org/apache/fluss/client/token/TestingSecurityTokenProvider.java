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

import org.apache.fluss.fs.token.ObtainedSecurityToken;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;

/** A {@link SecurityTokenProvider} for testing purpose. */
public class TestingSecurityTokenProvider implements SecurityTokenProvider {

    private final Queue<List<String>> historyTokens = new LinkedBlockingDeque<>();
    private volatile List<String> currentTokens;

    public TestingSecurityTokenProvider(List<String> currentTokens) {
        this.currentTokens = currentTokens;
    }

    public void setCurrentTokens(List<String> currentTokens) {
        synchronized (this) {
            this.currentTokens = currentTokens;
        }
    }

    @Override
    public List<ObtainedSecurityToken> obtainSecurityTokens() {
        synchronized (this) {
            List<String> previousTokens = historyTokens.peek();
            long currentTime = Clock.systemDefaultZone().millis();
            // we set expire time to 2s later, should be large enough for testing.
            // if it's too small, DefaultSecurityTokenManager#calculateRenewalDelay will
            // get a negative value by formula ‘Math.round(tokensRenewalTimeRatio * (nextRenewal -
            // now))’ which causes never renewal tokens
            long expireTime = currentTime + 2000;
            if (previousTokens != null && previousTokens.equals(currentTokens)) {
                // just return the previous tokens
                return previousTokens.stream()
                        .map(
                                t ->
                                        new ObtainedSecurityToken(
                                                "testing",
                                                t.getBytes(),
                                                expireTime,
                                                Collections.emptyMap()))
                        .collect(Collectors.toList());
            } else {
                // return the current tokens and push back to the queue
                historyTokens.add(currentTokens);
                return currentTokens.stream()
                        .map(
                                t ->
                                        new ObtainedSecurityToken(
                                                "testing",
                                                t.getBytes(),
                                                expireTime,
                                                Collections.emptyMap()))
                        .collect(Collectors.toList());
            }
        }
    }

    public Queue<List<String>> getHistoryTokens() {
        return historyTokens;
    }
}
