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

package com.alibaba.fluss.client.token;

import com.alibaba.fluss.fs.token.ObtainedSecurityToken;

import java.time.Clock;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

/** A {@link SecurityTokenProvider} for testing purpose. */
public class TestingSecurityTokenProvider implements SecurityTokenProvider {

    private final Queue<String> historyTokens = new LinkedBlockingDeque<>();
    private volatile String currentToken;

    public TestingSecurityTokenProvider(String currentToken) {
        this.currentToken = currentToken;
    }

    public void setCurrentToken(String currentToken) {
        synchronized (this) {
            this.currentToken = currentToken;
        }
    }

    @Override
    public ObtainedSecurityToken obtainSecurityToken() {
        synchronized (this) {
            String previousToken = historyTokens.peek();
            long currentTime = Clock.systemDefaultZone().millis();
            // we set expire time to 2s later, should be large enough for testing.
            // if it's too small, DefaultSecurityTokenManager#calculateRenewalDelay will
            // get a negative value by formula ‘Math.round(tokensRenewalTimeRatio * (nextRenewal -
            // now))’ which causes never renewal token
            long expireTime = currentTime + 2000;
            if (previousToken != null && previousToken.equals(currentToken)) {
                // just return the previous one token
                return new ObtainedSecurityToken(
                        "testing", previousToken.getBytes(), expireTime, Collections.emptyMap());
            } else {
                // return the current token and push back to the queue
                historyTokens.add(currentToken);
                return new ObtainedSecurityToken(
                        "testing", currentToken.getBytes(), expireTime, Collections.emptyMap());
            }
        }
    }

    public Queue<String> getHistoryTokens() {
        return historyTokens;
    }
}
