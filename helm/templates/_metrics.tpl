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
{{- $reportersValue := $metrics.reporters | default "" | toString | trim -}}
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
Checks if prometheus reporter is enabled.
*/}}
{{- define "fluss.metrics.prometheusEnabled" -}}
{{- $reporterNames := include "fluss.metrics.reporterNames" . | fromYamlArray -}}
{{- if has "prometheus" $reporterNames -}}
true
{{- end -}}
{{- end -}}

{{/*
Renders metrics reporter configuration entries.
Expects the root context as argument.

From values:
  metrics:
    reporters: prometheus
    prometheus:
      port: 9249

Renders:
  metrics.reporters: prometheus
  metrics.reporter.prometheus.port: 9249

Only the port is rendered from values. All other reporter options
should be specified via configurationOverrides.
The port must be set via metrics.<name>.port, not via configurationOverrides.
*/}}
{{- define "fluss.metrics.config" -}}
{{- $config := .Values.configurationOverrides | default dict -}}
{{- $metrics := .Values.metrics | default dict -}}
{{- $reporterNames := include "fluss.metrics.reporterNames" . | fromYamlArray -}}
{{- if gt (len $reporterNames) 0 -}}
{{- if not (hasKey $config "metrics.reporters") }}
metrics.reporters: {{ join "," $reporterNames }}
{{- end -}}
{{- range $name := $reporterNames -}}
{{- $portKey := printf "metrics.reporter.%s.port" $name -}}
{{- if hasKey $config $portKey -}}
{{- fail (printf "metrics.reporter.%s.port must be set via metrics.%s.port in values.yaml, not via configurationOverrides" $name $name) -}}
{{- end -}}
{{- $reporterConfig := index $metrics $name | default dict -}}
{{- $port := $reporterConfig.port -}}
{{- if not $port -}}
{{- fail (printf "metrics.%s.port must be set when metrics.reporters includes %s" $name $name) -}}
{{- end }}
{{ $portKey }}: {{ $port }}
{{- end -}}
{{- end -}}
{{- end -}}
