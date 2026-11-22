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

package org.apache.fluss.security.auth.ssl;

import org.apache.fluss.security.auth.ssl.SslPrincipalMapper.NoMatchingRuleException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.regex.PatternSyntaxException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SslPrincipalMapper}. */
class SslPrincipalMapperTest {

    private static final String DN = "CN=alice,OU=eng,O=acme,L=city,ST=state,C=US";
    private static final String CN_RULE = "RULE:^CN=([^,]+),.*$/$1/";

    @Test
    void testDefaultKeepsFullDistinguishedName() throws Exception {
        assertThat(SslPrincipalMapper.fromRules("DEFAULT").getName(DN)).isEqualTo(DN);
        assertThat(SslPrincipalMapper.fromRules(null).getName(DN)).isEqualTo(DN);
        assertThat(SslPrincipalMapper.fromRules("  ").getName(DN)).isEqualTo(DN);
    }

    @Test
    void testExtractCommonName() throws Exception {
        assertThat(SslPrincipalMapper.fromRules(CN_RULE).getName(DN)).isEqualTo("alice");
    }

    @Test
    void testLowerAndUpperCasing() throws Exception {
        assertThat(SslPrincipalMapper.fromRules(CN_RULE + "U").getName(DN)).isEqualTo("ALICE");
        assertThat(SslPrincipalMapper.fromRules(CN_RULE + "L").getName("CN=ALICE,O=acme"))
                .isEqualTo("alice");
    }

    @Test
    void testFirstMatchingRuleWins() throws Exception {
        SslPrincipalMapper mapper =
                SslPrincipalMapper.fromRules("RULE:^CN=admin,.*$/superuser/, " + CN_RULE);
        assertThat(mapper.getName("CN=admin,O=acme")).isEqualTo("superuser");
        assertThat(mapper.getName(DN)).isEqualTo("alice");
    }

    @Test
    void testDefaultAsFallback() throws Exception {
        SslPrincipalMapper mapper = SslPrincipalMapper.fromRules(CN_RULE + ",DEFAULT");
        assertThat(mapper.getName(DN)).isEqualTo("alice");
        assertThat(mapper.getName("O=acme")).isEqualTo("O=acme");
    }

    @Test
    void testPatternMustMatchWholeName() {
        SslPrincipalMapper mapper = SslPrincipalMapper.fromRules("RULE:CN=([^,]+)/$1/");
        assertThatThrownBy(() -> mapper.getName(DN)).isInstanceOf(NoMatchingRuleException.class);
    }

    @Test
    void testNoMatchingRuleThrowsCheckedException() {
        SslPrincipalMapper mapper = SslPrincipalMapper.fromRules("RULE:^CN=admin,.*$/superuser/");
        assertThatThrownBy(() -> mapper.getName(DN))
                .isInstanceOf(NoMatchingRuleException.class)
                .hasMessageContaining(DN)
                .hasMessageContaining("RULE:^CN=admin,.*$/superuser/");
    }

    @Test
    void testEmptyResultDoesNotApply() throws Exception {
        String emptyCn = "CN=,O=acme";
        String rule = "RULE:^CN=([^,]*),.*$/$1/";
        assertThatThrownBy(() -> SslPrincipalMapper.fromRules(rule).getName(emptyCn))
                .isInstanceOf(NoMatchingRuleException.class);
        assertThat(SslPrincipalMapper.fromRules(rule + ",DEFAULT").getName(emptyCn))
                .isEqualTo(emptyCn);
    }

    @Test
    void testLiteralDollarInReplacement() throws Exception {
        assertThat(SslPrincipalMapper.fromRules("RULE:^CN=([^,]+),.*$/\\$$1/").getName(DN))
                .isEqualTo("$alice");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "NOTARULE:foo",
                "default",
                "RULE:^CN=(.*)$/$1",
                "RULE:^CN=(.*)$/$1/X",
                "RULE://$1/",
                CN_RULE + " DEFAULT",
            })
    void testInvalidGrammarRejected(String rules) {
        assertThatThrownBy(() -> SslPrincipalMapper.fromRules(rules))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid SSL principal mapping rules");
    }

    @Test
    void testEscapedSlash() throws Exception {
        SslPrincipalMapper mapper =
                SslPrincipalMapper.fromRules(
                        "RULE:^CN=([^,\\/]+)\\/([^,]+),.*$/$2\\/$1/, " + CN_RULE);
        assertThat(mapper.getName("CN=alice/admin,O=acme")).isEqualTo("admin/alice");
        assertThat(mapper.getName(DN)).isEqualTo("alice");
    }

    @Test
    void testInvalidPatternRejected() {
        assertThatThrownBy(() -> SslPrincipalMapper.fromRules("RULE:^CN=([^,]+.*$/$1/"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid pattern")
                .hasCauseInstanceOf(PatternSyntaxException.class);
    }

    @Test
    void testInvalidReplacementRejected() {
        assertThatThrownBy(() -> SslPrincipalMapper.fromRules("RULE:^CN=([^,]+),.*$/$2/"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("refers to group 2, but the pattern has 1");
        assertThatThrownBy(() -> SslPrincipalMapper.fromRules("RULE:^CN=(?<cn>[^,]+),.*$/${cn}/"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be followed by a group number");
    }
}
