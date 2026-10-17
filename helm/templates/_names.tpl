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
Name of a component resource: bare, or prefixed with "fluss.fullname" when
uniqueResourceNames is set.
Usage:
  include "fluss.resourceName" (dict "suffix" "coordinator-server" "context" .)
*/}}
{{- define "fluss.resourceName" -}}
{{- if .context.Values.uniqueResourceNames -}}
{{- printf "%s-%s" (include "fluss.fullname" .context) .suffix | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- .suffix -}}
{{- end -}}
{{- end -}}

{{/*
Name of the coordinator StatefulSet and PodDisruptionBudget.
*/}}
{{- define "fluss.coordinator.name" -}}
{{- include "fluss.resourceName" (dict "suffix" "coordinator-server" "context" .) -}}
{{- end -}}

{{/*
Name of the coordinator headless Service.
*/}}
{{- define "fluss.coordinator.serviceName" -}}
{{- include "fluss.resourceName" (dict "suffix" "coordinator-server-hs" "context" .) -}}
{{- end -}}

{{/*
Name of the tablet StatefulSet and PodDisruptionBudget.
*/}}
{{- define "fluss.tablet.name" -}}
{{- include "fluss.resourceName" (dict "suffix" "tablet-server" "context" .) -}}
{{- end -}}

{{/*
Name of the tablet headless Service.
*/}}
{{- define "fluss.tablet.serviceName" -}}
{{- include "fluss.resourceName" (dict "suffix" "tablet-server-hs" "context" .) -}}
{{- end -}}

{{/*
Name of the ConfigMap holding server.yaml. This is the one resource whose bare
name carries the chart name, so it does not follow "fluss.resourceName".
*/}}
{{- define "fluss.configMapName" -}}
{{- if .Values.uniqueResourceNames -}}
{{- printf "%s-conf-file" (include "fluss.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- "fluss-conf-file" -}}
{{- end -}}
{{- end -}}

{{/*
Name of the coordinator metrics headless Service. Already unique per release,
so the naming scheme leaves it alone.
*/}}
{{- define "fluss.coordinator.metricsServiceName" -}}
{{- printf "%s-coordinator-server-metrics-hs" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Name of the tablet metrics headless Service. Already unique per release, so the
naming scheme leaves it alone.
*/}}
{{- define "fluss.tablet.metricsServiceName" -}}
{{- printf "%s-tablet-server-metrics-hs" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Validates that the generated resource names stay within the 63 character limit
Kubernetes imposes on DNS labels. Only applies with uniqueResourceNames. The
longest generated name is a coordinator pod, which adds 19 characters for the
StatefulSet name plus up to 4 for the ordinal suffix.
Usage:
  include "fluss.names.validateError" .
*/}}
{{- define "fluss.names.validateError" -}}
{{- if .Values.uniqueResourceNames -}}
{{- $prefix := include "fluss.fullname" . -}}
{{- $longestSuffix := 23 -}}
{{- $maxPrefix := sub 63 $longestSuffix -}}
{{- if gt (len $prefix) (int $maxPrefix) -}}
{{- printf "resource name prefix %q is %d characters, but generated names must stay within 63, so the prefix must be at most %d. Shorten the release name or set fullnameOverride." $prefix (len $prefix) (int $maxPrefix) -}}
{{- end -}}
{{- end -}}
{{- end -}}
