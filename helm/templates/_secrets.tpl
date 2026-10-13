{{/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/}}

{{/*
Whether any secret mounts or env entries are configured.
Usage:
  include "fluss.secrets.enabled" .
*/}}
{{- define "fluss.secrets.enabled" -}}
{{- if or .Values.secrets.mounts .Values.secrets.env -}}
true
{{- end -}}
{{- end -}}

{{/*
Base directory under which each secret mount gets its own subdirectory.
*/}}
{{- define "fluss.secrets.basePath" -}}
{{- .Values.secrets.basePath | default "/etc/fluss/secrets" | trimSuffix "/" -}}
{{- end -}}

{{/*
Pod volumes for the configured secret mounts.
Usage:
  include "fluss.secrets.volumes" .
*/}}
{{- define "fluss.secrets.volumes" -}}
{{- range .Values.secrets.mounts }}
- name: secret-{{ .name }}
  secret:
    secretName: {{ .secretName }}
{{- end }}
{{- end -}}

{{/*
Read-only container volumeMounts, one subdirectory per secret mount.
Usage:
  include "fluss.secrets.volumeMounts" .
*/}}
{{- define "fluss.secrets.volumeMounts" -}}
{{- range .Values.secrets.mounts }}
- name: secret-{{ .name }}
  mountPath: {{ include "fluss.secrets.basePath" $ }}/{{ .name }}
  readOnly: true
{{- end }}
{{- end -}}

{{/*
secretKeyRef env entries for the configured secret env vars.
Usage:
  include "fluss.secrets.env" .
*/}}
{{- define "fluss.secrets.env" -}}
{{- range .Values.secrets.env }}
- name: {{ .name }}
  valueFrom:
    secretKeyRef:
      name: {{ .secretName }}
      key: {{ .key }}
{{- end }}
{{- end -}}

{{/*
server.yaml lines enabling the config providers for the configured secrets,
restricted to the mounted subdirectories and declared env names.
Usage:
  include "fluss.secrets.configLines" .
*/}}
{{- define "fluss.secrets.configLines" -}}
{{- $providers := list -}}
{{- if .Values.secrets.mounts -}}
{{- $providers = append $providers "directory" -}}
{{- end -}}
{{- if .Values.secrets.env -}}
{{- $providers = append $providers "env" -}}
{{- end -}}
config.providers: {{ join "," $providers }}
{{- if .Values.secrets.mounts }}
{{- $paths := list }}
{{- range .Values.secrets.mounts }}
{{- $paths = append $paths (printf "%s/%s" (include "fluss.secrets.basePath" $) .name) }}
{{- end }}
config.providers.directory.param.allowed.paths: {{ join "," $paths }}
{{- end }}
{{- if .Values.secrets.env }}
{{- $names := list }}
{{- range .Values.secrets.env }}
{{- $names = append $names .name }}
{{- end }}
config.providers.env.param.allowlist.pattern: {{ join "|" $names }}
{{- end }}
{{- end -}}

{{/*
Collects secrets error messages.
Usage:
  include "fluss.secrets.validateError" .
*/}}
{{- define "fluss.secrets.validateError" -}}
{{- $errMessages := list -}}
{{- if and .Values.secrets.mounts (not (hasPrefix "/" (include "fluss.secrets.basePath" .))) -}}
{{- $errMessages = append $errMessages "secrets.basePath must be an absolute path." -}}
{{- end -}}
{{- $names := list -}}
{{- range .Values.secrets.mounts -}}
{{- if or (not .name) (not .secretName) -}}
{{- $errMessages = append $errMessages "secrets.mounts entries require both 'name' and 'secretName'." -}}
{{- else if has .name $names -}}
{{- $errMessages = append $errMessages (printf "secrets.mounts contains duplicate name '%s'." .name) -}}
{{- else -}}
{{- $names = append $names .name -}}
{{- end -}}
{{- end -}}
{{- range .Values.secrets.env -}}
{{- if or (not .name) (not .secretName) (not .key) -}}
{{- $errMessages = append $errMessages "secrets.env entries require 'name', 'secretName' and 'key'." -}}
{{- end -}}
{{- end -}}
{{- $errMessages = without $errMessages "" -}}
{{- join "\n" $errMessages -}}
{{- end -}}
