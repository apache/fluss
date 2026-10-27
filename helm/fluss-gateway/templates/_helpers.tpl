{{/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/}}

{{- define "fluss-gateway.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Reserve space for resource suffixes such as -metrics and -config. */}}
{{- define "fluss-gateway.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 54 | trimSuffix "-" -}}
{{- else -}}
{{- $name := include "fluss-gateway.name" . -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 54 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 54 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "fluss-gateway.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fluss-gateway.name" . | quote }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
{{- end -}}

{{- define "fluss-gateway.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | quote }}
{{ include "fluss-gateway.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
{{- end -}}

{{- define "fluss-gateway.image" -}}
{{- $repository := .Values.image.repository -}}
{{- if .Values.image.registry -}}
{{- $repository = printf "%s/%s" .Values.image.registry $repository -}}
{{- end -}}
{{- printf "%s:%s" $repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}

{{- define "fluss-gateway.validate" -}}
{{- if le (int .Values.terminationGracePeriodSeconds) (add (int .Values.server.shutdownDrainTimeoutSeconds) 5) -}}
{{- fail "terminationGracePeriodSeconds must exceed server.shutdownDrainTimeoutSeconds + 5" -}}
{{- end -}}
{{- if and .Values.metrics.enabled (eq (int .Values.server.restPort) (int .Values.metrics.port)) -}}
{{- fail "server.restPort and metrics.port must differ" -}}
{{- end -}}
{{- range $id, $cluster := .Values.clusters -}}
{{- if eq (default "plaintext" $cluster.securityProtocol) "sasl" -}}
{{- if or (contains "__" $id) (hasSuffix "_" $id) -}}
{{- fail (printf "SASL cluster id %q must not contain consecutive underscores or end in an underscore" $id) -}}
{{- end -}}
{{- $sasl := default dict $cluster.sasl -}}
{{- $secret := default dict $sasl.existingSecret -}}
{{- $_ := required (printf "cluster %q sasl.existingSecret.name is required" $id) $secret.name -}}
{{- end -}}
{{- end -}}
{{- $managedKeys := list
    "gateway.clusters"
    "gateway.rest.listen"
    "gateway.rest.request-timeout"
    "gateway.security.authentication"
    "gateway.shutdown.drain-timeout"
    "gateway.metrics.enabled"
    "gateway.metrics.exporters"
    "gateway.metrics.exporter.prometheus.listen"
    "gateway.id"
    "gateway.instance-id"
    "gateway.host"
-}}
{{- range $key, $value := .Values.configurationOverrides -}}
{{- if or (has $key $managedKeys) (hasPrefix "gateway.cluster." $key) -}}
{{- fail (printf "configurationOverrides must not override chart-managed option %q" $key) -}}
{{- end -}}
{{- end -}}
{{- range $key := list "app.kubernetes.io/name" "app.kubernetes.io/instance" -}}
{{- if hasKey $.Values.podLabels $key -}}
{{- fail (printf "podLabels must not override selector label %q" $key) -}}
{{- end -}}
{{- end -}}
{{- if hasKey .Values.podAnnotations "checksum/config" -}}
{{- fail "podAnnotations must not override checksum/config" -}}
{{- end -}}
{{- end -}}

{{- define "fluss-gateway.configuration" -}}
{{- include "fluss-gateway.validate" . -}}
gateway.clusters: {{ keys .Values.clusters | sortAlpha | join "," | quote }}
gateway.rest.listen: {{ printf "0.0.0.0:%v" .Values.server.restPort | quote }}
gateway.rest.request-timeout: {{ .Values.server.requestTimeout | quote }}
gateway.security.authentication: trust
gateway.shutdown.drain-timeout: {{ printf "%ds" (int .Values.server.shutdownDrainTimeoutSeconds) | quote }}
gateway.metrics.enabled: {{ .Values.metrics.enabled }}
gateway.metrics.exporters: prometheus
gateway.metrics.exporter.prometheus.listen: {{ printf "0.0.0.0:%v" .Values.metrics.port | quote }}
{{- range $id := keys .Values.clusters | sortAlpha }}
{{- $cluster := index $.Values.clusters $id }}
gateway.cluster.{{ $id }}.bootstrap.servers: {{ $cluster.bootstrapServers | quote }}
gateway.cluster.{{ $id }}.connection.identity-mode: service
gateway.cluster.{{ $id }}.connection.security.protocol: {{ default "plaintext" $cluster.securityProtocol | quote }}
{{- end }}
{{- range $key, $value := .Values.configurationOverrides }}
{{ $key }}: {{ $value | toJson }}
{{- end }}
{{- end -}}
