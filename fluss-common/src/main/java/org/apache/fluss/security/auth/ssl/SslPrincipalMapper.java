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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.FlussException;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Maps the X.500 distinguished name of a client certificate, in the RFC 2253 form returned by
 * {@code X500Principal.getName()}, to a Fluss principal name.
 */
@Internal
public final class SslPrincipalMapper {

    private static final String DEFAULT = "DEFAULT";

    // A backslash escapes the character after it, so a pattern or replacement may contain '\/'.
    private static final Pattern RULE =
            Pattern.compile(
                    "\\s*(DEFAULT|RULE:((?:\\\\.|[^\\\\/])+)/((?:\\\\.|[^\\\\/])*)/([LU]?))"
                            + "\\s*(?:,|\\z)");

    private final List<Rule> rules;

    private SslPrincipalMapper(List<Rule> rules) {
        this.rules = rules;
    }

    /**
     * Parses the rules string, e.g. the value of {@code security.ssl.principal.mapping.rules}.
     *
     * @throws IllegalArgumentException if the string is not a list of rules this class can apply
     */
    public static SslPrincipalMapper fromRules(@Nullable String rulesString) {
        if (rulesString == null || rulesString.trim().isEmpty()) {
            return new SslPrincipalMapper(Collections.singletonList(Rule.DEFAULT));
        }
        List<Rule> rules = new ArrayList<>();
        Matcher matcher = RULE.matcher(rulesString);
        int position = 0;
        while (position < rulesString.length()) {
            if (!matcher.find(position) || matcher.start() != position) {
                throw new IllegalArgumentException(
                        String.format(
                                "Invalid SSL principal mapping rules '%s' at position %d: expected "
                                        + "a comma-separated list of 'DEFAULT' or "
                                        + "'RULE:pattern/replacement/[LU]'.",
                                rulesString, position));
            }
            if (DEFAULT.equals(matcher.group(1))) {
                rules.add(Rule.DEFAULT);
            } else {
                rules.add(
                        Rule.parse(
                                matcher.group(1),
                                matcher.group(2),
                                matcher.group(3),
                                matcher.group(4)));
            }
            position = matcher.end();
        }
        return new SslPrincipalMapper(rules);
    }

    /**
     * Maps a distinguished name to a principal name with the first rule that applies.
     *
     * @throws NoMatchingRuleException if no rule applies to the distinguished name
     */
    public String getName(String distinguishedName) throws NoMatchingRuleException {
        checkNotNull(distinguishedName, "distinguishedName must not be null");
        for (Rule rule : rules) {
            String principalName = rule.apply(distinguishedName);
            if (principalName != null) {
                return principalName;
            }
        }
        throw new NoMatchingRuleException(
                String.format(
                        "No SSL principal mapping rule applies to the distinguished name '%s', "
                                + "rules: %s",
                        distinguishedName, rules));
    }

    @Override
    public String toString() {
        return "SslPrincipalMapper(rules=" + rules + ")";
    }

    /** Thrown by {@link #getName} when no rule applies to a distinguished name. */
    public static final class NoMatchingRuleException extends FlussException {
        private static final long serialVersionUID = 1L;

        public NoMatchingRuleException(String message) {
            super(message);
        }
    }

    private static final class Rule {
        private static final Rule DEFAULT = new Rule(null, null, false, false);

        @Nullable private final Pattern pattern;
        @Nullable private final String replacement;
        private final boolean toLowerCase;
        private final boolean toUpperCase;

        private Rule(
                @Nullable Pattern pattern,
                @Nullable String replacement,
                boolean toLowerCase,
                boolean toUpperCase) {
            this.pattern = pattern;
            this.replacement = replacement;
            this.toLowerCase = toLowerCase;
            this.toUpperCase = toUpperCase;
        }

        static Rule parse(String rule, String pattern, String replacement, String caseFlag) {
            Pattern compiled;
            try {
                compiled = Pattern.compile(pattern);
            } catch (PatternSyntaxException e) {
                throw new IllegalArgumentException(
                        String.format(
                                "Invalid SSL principal mapping rule '%s': invalid pattern.", rule),
                        e);
            }
            checkReplacement(rule, replacement, compiled.matcher("").groupCount());
            return new Rule(compiled, replacement, "L".equals(caseFlag), "U".equals(caseFlag));
        }

        private static void checkReplacement(String rule, String replacement, int groupCount) {
            for (int i = 0; i < replacement.length(); i++) {
                char c = replacement.charAt(i);
                if (c == '\\') {
                    // Skips the escaped character. The RULE grammar pairs every backslash with
                    // the character after it, so a trailing backslash cannot occur.
                    i++;
                } else if (c == '$') {
                    if (i + 1 == replacement.length()
                            || !Character.isDigit(replacement.charAt(i + 1))) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "Invalid SSL principal mapping rule '%s': '$' in the "
                                                + "replacement must be followed by a group "
                                                + "number; escape a literal '$' as '\\$'.",
                                        rule));
                    }
                    int group = replacement.charAt(i + 1) - '0';
                    if (group > groupCount) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "Invalid SSL principal mapping rule '%s': the replacement "
                                                + "refers to group %d, but the pattern has %d.",
                                        rule, group, groupCount));
                    }
                }
            }
        }

        @Nullable
        String apply(String distinguishedName) {
            if (pattern == null) {
                return distinguishedName;
            }
            Matcher matcher = pattern.matcher(distinguishedName);
            if (!matcher.matches()) {
                return null;
            }
            // The match covers the whole name, so the replacement is the whole result.
            StringBuffer result = new StringBuffer();
            matcher.appendReplacement(result, replacement);
            String principalName = result.toString();
            if (toLowerCase) {
                principalName = principalName.toLowerCase(Locale.ROOT);
            } else if (toUpperCase) {
                principalName = principalName.toUpperCase(Locale.ROOT);
            }
            return principalName.isEmpty() ? null : principalName;
        }

        @Override
        public String toString() {
            if (pattern == null) {
                return SslPrincipalMapper.DEFAULT;
            }
            return "RULE:"
                    + pattern
                    + "/"
                    + replacement
                    + "/"
                    + (toLowerCase ? "L" : toUpperCase ? "U" : "");
        }
    }
}
