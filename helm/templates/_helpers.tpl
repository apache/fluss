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
Expand the name of the chart.
*/}}
{{- define "fluss.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "fluss.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "fluss.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "fluss.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name (.Chart.Version | replace "+" "_") | quote }}
app.kubernetes.io/name: {{ include "fluss.name" . | quote }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
{{- end -}}

{{/*
Selector labels
*/}}
{{- define "fluss.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fluss.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Image name
*/}}
{{- define "fluss.image" -}}

{{- $registry := .Values.image.registry | default "" -}}
{{- $repo := required "image.repository is required" .Values.image.repository -}}
{{- $tag := .Values.image.tag | default "" | toString -}}

{{- $image := $repo -}}

{{- if $registry -}}
  {{- $image = printf "%s/%s" $registry $image -}}
{{- end -}}

{{- if $tag -}}
  {{- $image = printf "%s:%s" $image $tag -}}
{{- end -}}

{{- $image -}}
{{- end -}}

{{/*
Render pull secrets
*/}}
{{- define "fluss.imagePullSecrets" -}}
{{- $secrets := .Values.image.pullSecrets -}}
{{- if $secrets -}}
imagePullSecrets:
{{- range $secrets }}
  - name: {{ . }}
{{- end }}
{{- end }}
{{- end }}

{{/*
  Validate PodDisruptionBudget for a given component.
  Usage: include "fluss.pdb.validate" (dict "component" "tablet" "pdb" .Values.tablet.podDisruptionBudget)
*/}}
{{- define "fluss.pdb.validate" -}}
{{- if .pdb.enabled -}}
  {{- $hasMin := hasKey .pdb "minAvailable" -}}
  {{- $hasMax := hasKey .pdb "maxUnavailable" -}}
  {{- if and $hasMin $hasMax -}}
    {{- printf "%s.podDisruptionBudget: cannot set both minAvailable and maxUnavailable" .component -}}
  {{- else if not (or $hasMin $hasMax) -}}
    {{- printf "%s.podDisruptionBudget: must set either minAvailable or maxUnavailable when enabled" .component -}}
  {{- end -}}
{{- end -}}
{{- end -}}

{{/*
  Aggregate PDB validation for all components.
  include "fluss.pdb.validateValues" .
*/}}
{{- define "fluss.pdb.validateValues" -}}
{{- $errMessages := list -}}
{{- $errMessages = append $errMessages (include "fluss.pdb.validate" (dict "component" "tablet" "pdb" .Values.tablet.podDisruptionBudget)) -}}
{{- $errMessages = append $errMessages (include "fluss.pdb.validate" (dict "component" "coordinator" "pdb" .Values.coordinator.podDisruptionBudget)) -}}
{{- $errMessages = without $errMessages "" -}}
{{- $errMessage := join "\n" $errMessages -}}
{{- if $errMessage -}}
{{-   printf "\nPDB VALIDATION:\n%s" $errMessage | fail -}}
{{- end -}}
{{- end -}}


{{/*
Env entries publishing each listener's advertised address as
FLUSS_ADVERTISED_LISTENER_<NAME>=<host>:<port>, one per entry in .Values.listeners.
Fluss advertises a listener missing from advertised.listeners at its bind address,
so INTERNAL is published as the pod IP; the others use the pod's headless-service DNS name.
The values reference POD_* via $(VAR), so they must be rendered after those entries.
Usage: include "fluss.listeners.advertisedEnv" (dict "root" . "service" "coordinator-server-hs")
*/}}
{{- define "fluss.listeners.advertisedEnv" -}}
{{- $service := .service -}}
{{- range $name, $listener := .root.Values.listeners }}
- name: FLUSS_ADVERTISED_LISTENER_{{ upper $name }}
  {{- if eq $name "internal" }}
  value: $(POD_IP):{{ $listener.port }}
  {{- else }}
  value: $(POD_NAME).{{ $service }}.$(POD_NAMESPACE).svc.cluster.local:{{ $listener.port }}
  {{- end }}
{{- end }}
{{- end -}}

{{/*
bind.listeners value: every listener bound on the pod IP.
*/}}
{{- define "fluss.listeners.bind" -}}
{{- $out := list -}}
{{- range $name, $listener := .Values.listeners -}}
{{- $out = append $out (printf "%s://${POD_IP}:%v" (upper $name) $listener.port) -}}
{{- end -}}
{{- join ", " $out -}}
{{- end -}}

{{/*
advertised.listeners value, read from the env entries of "fluss.listeners.advertisedEnv"
so the published addresses and the configured ones cannot differ.
*/}}
{{- define "fluss.listeners.advertised" -}}
{{- $out := list -}}
{{- range $name, $_ := .Values.listeners -}}
{{- $out = append $out (printf "%s://${FLUSS_ADVERTISED_LISTENER_%s}" (upper $name) (upper $name)) -}}
{{- end -}}
{{- join ", " $out -}}
{{- end -}}
