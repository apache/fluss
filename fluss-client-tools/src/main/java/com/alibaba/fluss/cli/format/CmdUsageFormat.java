/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.cli.format;

import com.alibaba.fluss.annotation.Internal;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.JCommander.ProgramName;
import com.beust.jcommander.ParameterDescription;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.Strings;
import com.beust.jcommander.UnixStyleUsageFormatter;
import com.beust.jcommander.WrappedParameter;
import java.util.EnumSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A Cmd usage formatter. This works by overriding and modifying the output of {@link
 * #appendCommands(StringBuilder, int, int, String)} and {@link
 * #appendAllParametersDetails(StringBuilder, int, String, List)} which is inherited from {@link
 * UnixStyleUsageFormatter}.
 */
@Internal
public class CmdUsageFormat extends UnixStyleUsageFormatter {

    private final JCommander commander;

    public CmdUsageFormat(JCommander commander) {
        super(commander);
        this.commander = commander;
    }

    @Override
    public void appendCommands(
            StringBuilder out, int indentCount, int descriptionIndent, String indent) {
        List<Entry<ProgramName, JCommander>> visibleCommands =
                commander.getRawCommands().entrySet().stream()
                        .filter(
                                entry -> {
                                    Object arg = entry.getValue().getObjects().get(0);
                                    Parameters p = arg.getClass().getAnnotation(Parameters.class);
                                    return Objects.isNull(p) || !p.hidden();
                                })
                        .collect(Collectors.toList());
        if (visibleCommands.isEmpty()) {
            return;
        }
        out.append(indent).append(s(2)).append("Commands:\n");
        int maxDispNameLength =
                visibleCommands.stream()
                        .map(entry -> entry.getKey().getDisplayName())
                        .mapToInt(String::length)
                        .max()
                        .orElse(0);
        int spaceBetweenNameAndDesc = 8;
        for (Entry<ProgramName, JCommander> entry : visibleCommands) {
            JCommander.ProgramName progName = entry.getKey();
            String dispName = progName.getDisplayName();
            String description = getCommandDescription(progName.getName());
            String line =
                    indent
                            + s(4)
                            + dispName
                            + s(maxDispNameLength - dispName.length() + spaceBetweenNameAndDesc)
                            + description;
            wrapDescription(out, indentCount + descriptionIndent, line);
            out.append("\n");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void appendAllParametersDetails(
            StringBuilder out,
            int indentCount,
            String indent,
            List<ParameterDescription> sortedParameters) {
        if (!sortedParameters.isEmpty()) {
            out.append(indent).append("  Options:\n");
        }

        int prefixIndent =
                sortedParameters.stream()
                        .mapToInt(
                                pd -> {
                                    WrappedParameter param = pd.getParameter();
                                    return (param.required() ? "* " : "  ").length()
                                            + pd.getNames().length();
                                })
                        .max()
                        .orElse(0);

        // Append parameters
        for (ParameterDescription pd : sortedParameters) {
            WrappedParameter parameter = pd.getParameter();

            String prefix = (parameter.required() ? "* " : "  ") + pd.getNames();
            out.append(indent)
                    .append("  ")
                    .append(prefix)
                    .append(s(prefixIndent - prefix.length()))
                    .append(s(8));
            final int initialLinePrefixLength = indent.length() + prefixIndent + 3;

            String description = pd.getDescription();
            Object def = pd.getDefault();

            if (pd.isDynamicParameter()) {
                String syntax =
                        "(syntax: "
                                + parameter.names()[0]
                                + "key"
                                + parameter.getAssignment()
                                + "value)";
                description += (description.isEmpty() ? "" : " ") + syntax;
            }

            if (def != null && !pd.isHelp()) {
                String displayedDef =
                        Strings.isStringEmpty(def.toString()) ? "<empty string>" : def.toString();
                String defaultText =
                        "(default: " + (parameter.password() ? "********" : displayedDef) + ")";
                description += (description.isEmpty() ? "" : " ") + defaultText;
            }
            Class<?> type = pd.getParameterized().getType();

            if (type.isEnum()) {
                String valueList = EnumSet.allOf((Class<? extends Enum>) type).toString();
                if (!description.contains("Options: " + valueList)) {
                    String possibleValues = "(values: " + valueList + ")";
                    description += (description.isEmpty() ? "" : " ") + possibleValues;
                }
            }
            wrapDescription(
                    out, indentCount + prefixIndent - 3, initialLinePrefixLength, description);
            out.append("\n");
        }
    }
}
