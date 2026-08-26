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
Effective tablet storage volumes as a YAML list of {name, size, storageClass}:
tablet.storage.volumes when non-empty, otherwise a single legacy entry built from
tablet.storage.size / tablet.storage.storageClass. Each volume is mounted at
/tmp/fluss/<name>, so the legacy entry keeps the original /tmp/fluss/data path.
Usage:
  include "fluss.tablet.storage.volumes" . | fromYamlArray
*/}}
{{- define "fluss.tablet.storage.volumes" -}}
{{- if .Values.tablet.storage.volumes -}}
{{- toYaml .Values.tablet.storage.volumes -}}
{{- else -}}
- name: data
  size: {{ .Values.tablet.storage.size }}
  storageClass: {{ .Values.tablet.storage.storageClass }}
{{- end -}}
{{- end -}}

{{/*
Comma-separated data.dirs value for server.yaml, derived from the effective volume
mount paths.
Usage:
  include "fluss.tablet.storage.dataDirs" .
*/}}
{{- define "fluss.tablet.storage.dataDirs" -}}
{{- $paths := list -}}
{{- range (include "fluss.tablet.storage.volumes" . | fromYamlArray) -}}
{{- $paths = append $paths (printf "/tmp/fluss/%s" .name) -}}
{{- end -}}
{{- join "," $paths -}}
{{- end -}}

{{/*
Validation errors for tablet.storage.volumes.
Usage:
  include "fluss.storage.validateError" .
*/}}
{{- define "fluss.storage.validateError" -}}
{{- $messages := list -}}
{{- $names := list -}}
{{- range .Values.tablet.storage.volumes -}}
  {{- if not .name -}}
    {{- $messages = append $messages "tablet.storage.volumes: every entry must set name" -}}
  {{- else -}}
    {{- if has .name $names -}}
      {{- $messages = append $messages (printf "tablet.storage.volumes: duplicate volume name %q" .name) -}}
    {{- end -}}
    {{- if or (eq .name "fluss-conf") (eq .name "sasl-config") (hasPrefix "secret-" .name) -}}
      {{- $messages = append $messages (printf "tablet.storage.volumes: volume name %q collides with a chart-managed volume (fluss-conf, sasl-config, secret-*)" .name) -}}
    {{- end -}}
    {{- if not .size -}}
      {{- $messages = append $messages (printf "tablet.storage.volumes: entry %q must set size" .name) -}}
    {{- end -}}
    {{- $names = append $names .name -}}
  {{- end -}}
{{- end -}}
{{- if and .Values.tablet.storage.volumes (hasKey .Values.configurationOverrides "data.dirs") -}}
  {{- $messages = append $messages "configurationOverrides must not set data.dirs when tablet.storage.volumes is used: the chart renders data.dirs from the volume list" -}}
{{- end -}}
{{- join "\n" $messages -}}
{{- end -}}
