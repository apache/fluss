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
Render metrics reporter configuration entries.
Expects the root context as argument.

From values:
  metrics:
    reporters:
      prometheus:
        port: 9249

Renders:
  metrics.reporters: prometheus
  metrics.reporter.prometheus.port: 9249

Keys already present in configurationOverrides are not rendered.
*/}}
{{- define "fluss.metrics.config" -}}
{{- $config := .Values.configurationOverrides | default dict -}}
{{- $reporters := .Values.metrics.reporters | default dict -}}
{{- $reporterNames := keys $reporters | sortAlpha -}}
{{- if eq (len $reporterNames) 0 -}}
{{- fail "metrics.reporters must contain at least one reporter when metrics.enabled is true" -}}
{{- end -}}
{{- if not (hasKey $config "metrics.reporters") }}
metrics.reporters: {{ join "," $reporterNames }}
{{- end -}}
{{- range $name := $reporterNames -}}
{{- range $option, $value := index $reporters $name -}}
{{- $fullKey := printf "metrics.reporter.%s.%s" $name $option -}}
{{- if not (hasKey $config $fullKey) }}
{{ $fullKey }}: {{ tpl (printf "%v" $value) $ }}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
