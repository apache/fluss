#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

{{/*
Returns list of provided reporter names.
*/}}
{{- define "fluss.metrics.reporterNames" -}}
{{- $metrics := .Values.metrics | default dict -}}
{{- $reportersValue := (index $metrics "reporters" | default "") | toString | trim -}}
{{- if eq $reportersValue "" -}}
[]
{{- else -}}
{{- $selected := list -}}
{{- range $raw := regexSplit "\\s*,\\s*" $reportersValue -1 -}}
{{- $name := trim $raw -}}
{{- if ne $name "" -}}
{{- $selected = append $selected $name -}}
{{- end -}}
{{- end -}}
{{- $selected | toYaml -}}
{{- end -}}
{{- end -}}

{{/*
Checks if metrics reporting is enabled, with any one of the reporters.
*/}}
{{- define "fluss.metrics.enabled" -}}
{{- $reporters := include "fluss.metrics.reporterNames" . | fromYamlArray -}}
{{- if gt (len $reporters) 0 -}}
true
{{- end -}}
{{- end -}}

{{/*
Checks if a specific reporter is enabled.
Usage:
  {{ include "fluss.metrics.reporter.enabled" (dict "reporter" "prometheus" "ctx" .) }}
  {{ include "fluss.metrics.reporter.enabled" (dict "reporter" "jmx"        "ctx" .) }}
*/}}
{{- define "fluss.metrics.reporter.enabled" -}}
{{- $reporterNames := include "fluss.metrics.reporterNames" .ctx | fromYamlArray -}}
{{- if has .reporter $reporterNames -}}
true
{{- end -}}
{{- end -}}

{{/*
Checks if prometheus reporter is enabled.
*/}}
{{- define "fluss.metrics.reporter.prometheus.enabled" -}}
{{- include "fluss.metrics.reporter.enabled" (dict "reporter" "prometheus" "ctx" .) -}}
{{- end -}}

{{/*
Checks if jmx reporter is enabled.
*/}}
{{- define "fluss.metrics.reporter.jmx.enabled" -}}
{{- include "fluss.metrics.reporter.enabled" (dict "reporter" "jmx" "ctx" .) -}}
{{- end -}}

{{/*
Validates that each enabled reporter has a port configured.
*/}}
{{- define "fluss.metrics.validateReporterPorts" -}}
{{- $metrics := .Values.metrics | default dict -}}
{{- $reporterNames := include "fluss.metrics.reporterNames" . | fromYamlArray -}}
{{- range $name := $reporterNames -}}
  {{- $reporterConfig := index $metrics $name | default dict -}}
  {{- if not $reporterConfig.port -}}
    {{- printf "metrics.%s.port must be set when metrics.reporters includes %s" $name $name -}}
  {{- end -}}
{{- end -}}
{{- end -}}

{{/*
Validates that reporter ports are not set via configurationOverrides.
*/}}
{{- define "fluss.metrics.validateNoPortOverrides" -}}
{{- $config := .Values.configurationOverrides | default dict -}}
{{- $reporterNames := include "fluss.metrics.reporterNames" . | fromYamlArray -}}
{{- range $name := $reporterNames -}}
  {{- $portKey := printf "metrics.reporter.%s.port" $name -}}
  {{- if hasKey $config $portKey -}}
    {{- printf "metrics.reporter.%s.port must be set via metrics.%s.port in values.yaml, not via configurationOverrides" $name $name -}}
  {{- end -}}
{{- end -}}
{{- end -}}

{{/*
Validates that metrics.reporters is not set via configurationOverrides.
*/}}
{{- define "fluss.metrics.validateNoReportersOverrides" -}}
{{- $config := .Values.configurationOverrides | default dict -}}
{{- if hasKey $config "metrics.reporters" -}}
  {{- printf "metrics.reporters must be set via metrics.reporters in values.yaml, not via configurationOverrides" -}}
{{- end -}}
{{- end -}}

{{/*
Validates metrics configuration values.
Usage:
  include "fluss.metrics.validateValues" .
*/}}
{{- define "fluss.metrics.validateValues" -}}

{{- $errMessages := list -}}
{{- $errMessages = append $errMessages (include "fluss.metrics.validateReporterPorts" .) -}}
{{- $errMessages = append $errMessages (include "fluss.metrics.validateNoPortOverrides" .) -}}
{{- $errMessages = append $errMessages (include "fluss.metrics.validateNoReportersOverrides" .) -}}

{{- $errMessages = without $errMessages "" -}}
{{- $errMessage := join "\n" $errMessages -}}

{{- if $errMessage -}}
{{-   printf "\nVALUES VALIDATION:\n%s" $errMessage | fail -}}
{{- end -}}

{{- end -}}

