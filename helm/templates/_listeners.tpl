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
Returns "true" when the optional EXTERNAL listener is enabled.
Usage:
  include "fluss.listeners.external.enabled" .
*/}}
{{- define "fluss.listeners.external.enabled" -}}
{{- $ext := .Values.listeners.external | default dict -}}
{{- if $ext.enabled -}}true{{- end -}}
{{- end -}}

{{/*
Hardcoded Fluss listener name for the extra out-of-cluster listener.
Usage:
  include "fluss.listeners.external.name" .
*/}}
{{- define "fluss.listeners.external.name" -}}
EXTERNAL
{{- end -}}

{{/*
Bind port for the optional EXTERNAL listener.
Usage:
  include "fluss.listeners.external.port" .
*/}}
{{- define "fluss.listeners.external.port" -}}
{{- $ext := .Values.listeners.external | default dict -}}
{{- $ext.port | default 9125 -}}
{{- end -}}

{{/*
Kubernetes cluster domain used in chart-owned CLIENT advertised FQDNs.
Usage:
  include "fluss.listeners.clusterDomain" .
*/}}
{{- define "fluss.listeners.clusterDomain" -}}
{{- .Values.clusterDomain | default "cluster.local" | toString | trim -}}
{{- end -}}

{{/*
Per-ordinal advertised hosts for a component (tablet only). Empty list if unset.
Usage:
  include "fluss.listeners.external.hostsYaml" (dict "ctx" . "component" "tablet")
*/}}
{{- define "fluss.listeners.external.hostsYaml" -}}
{{- $comp := index .ctx.Values .component | default dict -}}
{{- $listeners := index $comp "listeners" | default dict -}}
{{- $ext := index $listeners "external" | default dict -}}
{{- $hosts := index $ext "advertisedHosts" | default list -}}
{{- if kindIs "slice" $hosts -}}
{{- $hosts | toYaml -}}
{{- else -}}
[]
{{- end -}}
{{- end -}}

{{/*
Per-ordinal advertised ports for a component (tablet only). Empty list if unset.
Usage:
  include "fluss.listeners.external.portsYaml" (dict "ctx" . "component" "tablet")
*/}}
{{- define "fluss.listeners.external.portsYaml" -}}
{{- $comp := index .ctx.Values .component | default dict -}}
{{- $listeners := index $comp "listeners" | default dict -}}
{{- $ext := index $listeners "external" | default dict -}}
{{- $ports := index $ext "advertisedPorts" | default list -}}
{{- if kindIs "slice" $ports -}}
{{- $ports | toYaml -}}
{{- else -}}
[]
{{- end -}}
{{- end -}}

{{/*
Scalar advertised host/port after per-component override, global fallback, and helm tpl.
Empty means "use the chart default" (bind port for advertised ports).
Usage:
  include "fluss.listeners.external.scalar" (dict "ctx" . "component" "tablet" "field" "advertisedHost")
*/}}
{{- define "fluss.listeners.external.scalar" -}}
{{- $comp := index .ctx.Values .component | default dict -}}
{{- $listeners := index $comp "listeners" | default dict -}}
{{- $ext := index $listeners "external" | default dict -}}
{{- $global := .ctx.Values.listeners.external | default dict -}}
{{- $compVal := index $ext .field | default "" | toString | trim -}}
{{- $globalVal := index $global .field | default "" | toString | trim -}}
{{- $raw := $compVal | default $globalVal -}}
{{- if $raw -}}
{{- tpl $raw .ctx | trim -}}
{{- end -}}
{{- end -}}

{{/*
True when advertisedHost is the whitelisted runtime node-IP token.
Usage:
  include "fluss.listeners.isNodeIpToken" $host
*/}}
{{- define "fluss.listeners.isNodeIpToken" -}}
{{- if eq (. | toString | trim) "${NODE_IP}" -}}true{{- end -}}
{{- end -}}

{{/*
True when a host value is NOT allowed. Allowed: a literal hostname/IP (also via
helm tpl) or exactly ${NODE_IP}. Shell tokens, commas, and whitespace are
rejected: hosts are spliced into comma-separated server config and the
per-ordinal CSV, so they would silently corrupt both.
Usage:
  include "fluss.listeners.invalidHostToken" $host
*/}}
{{- define "fluss.listeners.invalidHostToken" -}}
{{- $host := . | toString | trim -}}
{{- if and $host (or (and (contains "$" $host) (ne $host "${NODE_IP}")) (contains "," $host) (contains " " $host)) -}}
true
{{- end -}}
{{- end -}}

{{/*
True when a port value is set and is not a decimal integer.
Usage:
  include "fluss.listeners.invalidPortToken" $port
*/}}
{{- define "fluss.listeners.invalidPortToken" -}}
{{- $port := . | toString | trim -}}
{{- if and $port (not (regexMatch "^[0-9]+$" $port)) -}}
true
{{- end -}}
{{- end -}}

{{/*
Resolved EXTERNAL advertised host expression written into advertised.listeners.
Per-ordinal hosts become $EXTERNAL_ADVERTISED_HOST (filled at pod start).
Usage:
  include "fluss.listeners.external.hostExpr" (dict "ctx" . "component" "tablet")
*/}}
{{- define "fluss.listeners.external.hostExpr" -}}
{{- $hosts := include "fluss.listeners.external.hostsYaml" . | fromYamlArray -}}
{{- if gt (len $hosts) 0 -}}
$EXTERNAL_ADVERTISED_HOST
{{- else -}}
{{- include "fluss.listeners.external.scalar" (dict "ctx" .ctx "component" .component "field" "advertisedHost") -}}
{{- end -}}
{{- end -}}

{{/*
Resolved EXTERNAL advertised port expression written into advertised.listeners.
Per-ordinal ports become $EXTERNAL_ADVERTISED_PORT (filled at pod start).
Usage:
  include "fluss.listeners.external.portExpr" (dict "ctx" . "component" "tablet")
*/}}
{{- define "fluss.listeners.external.portExpr" -}}
{{- $ports := include "fluss.listeners.external.portsYaml" . | fromYamlArray -}}
{{- if gt (len $ports) 0 -}}
$EXTERNAL_ADVERTISED_PORT
{{- else -}}
{{- $raw := include "fluss.listeners.external.scalar" (dict "ctx" .ctx "component" .component "field" "advertisedPort") -}}
{{- if $raw -}}
{{- $raw -}}
{{- else -}}
{{- include "fluss.listeners.external.port" .ctx -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
CSV of per-ordinal advertised hosts, each run through helm tpl.
Usage:
  include "fluss.listeners.external.hostsCsv" (dict "ctx" . "component" "tablet")
*/}}
{{- define "fluss.listeners.external.hostsCsv" -}}
{{- $ctx := .ctx -}}
{{- $items := list -}}
{{- range $h := include "fluss.listeners.external.hostsYaml" . | fromYamlArray -}}
{{- $items = append $items (tpl ($h | toString) $ctx | trim) -}}
{{- end -}}
{{- join "," $items -}}
{{- end -}}

{{/*
CSV of per-ordinal advertised ports.
Usage:
  include "fluss.listeners.external.portsCsv" (dict "ctx" . "component" "tablet")
*/}}
{{- define "fluss.listeners.external.portsCsv" -}}
{{- $items := list -}}
{{- range $p := include "fluss.listeners.external.portsYaml" . | fromYamlArray -}}
{{- $items = append $items ($p | toString | trim) -}}
{{- end -}}
{{- join "," $items -}}
{{- end -}}

{{/*
One-line shell snippet that picks this pod's ordinal entry from advertisedHosts/Ports.
Empty when this component does not use per-ordinal arrays.
Usage:
  include "fluss.listeners.external.ordinalSetup" (dict "ctx" . "component" "tablet")
*/}}
{{- define "fluss.listeners.external.ordinalSetup" -}}
{{- $hostsCsv := include "fluss.listeners.external.hostsCsv" . -}}
{{- $portsCsv := include "fluss.listeners.external.portsCsv" . -}}
{{- $parts := list -}}
{{- if $hostsCsv -}}
{{- $parts = append $parts (printf "EXTERNAL_ADVERTISED_HOSTS=%s" ($hostsCsv | quote)) -}}
{{- $parts = append $parts "EXTERNAL_ADVERTISED_HOST=$(printf '%s' \"$EXTERNAL_ADVERTISED_HOSTS\" | awk -F, -v i=\"$FLUSS_SERVER_ID\" '{print $(i+1)}')" -}}
{{- end -}}
{{- if $portsCsv -}}
{{- $parts = append $parts (printf "EXTERNAL_ADVERTISED_PORTS=%s" ($portsCsv | quote)) -}}
{{- $parts = append $parts "EXTERNAL_ADVERTISED_PORT=$(printf '%s' \"$EXTERNAL_ADVERTISED_PORTS\" | awk -F, -v i=\"$FLUSS_SERVER_ID\" '{print $(i+1)}')" -}}
{{- end -}}
{{- join "; " $parts -}}
{{- end -}}

{{/*
bind.listeners value written into server.yaml at pod start.
Usage:
  include "fluss.listeners.bind" .
*/}}
{{- define "fluss.listeners.bind" -}}
{{- $parts := list (printf "INTERNAL://${POD_IP}:%v" .Values.listeners.internal.port) (printf "CLIENT://${POD_IP}:%v" .Values.listeners.client.port) -}}
{{- if (include "fluss.listeners.external.enabled" .) -}}
{{- $parts = append $parts (printf "%s://${POD_IP}:%v" (include "fluss.listeners.external.name" .) (include "fluss.listeners.external.port" .)) -}}
{{- end -}}
{{- join ", " $parts -}}
{{- end -}}

{{/*
advertised.listeners value written into server.yaml at pod start.
CLIENT addressing is chart-owned (pod FQDN on clusterDomain). EXTERNAL uses
resolved host/port expressions (literals, ${NODE_IP}, or ordinal-array vars).
Usage:
  include "fluss.listeners.advertised" (dict "ctx" . "component" "tablet")
*/}}
{{- define "fluss.listeners.advertised" -}}
{{- $ctx := .ctx -}}
{{- $component := .component -}}
{{- $headless := ternary "tablet-server-hs" "coordinator-server-hs" (eq $component "tablet") -}}
{{- $domain := include "fluss.listeners.clusterDomain" $ctx -}}
{{- $clientPort := $ctx.Values.listeners.client.port -}}
{{- $clientHost := printf "${POD_NAME}.%s.${POD_NAMESPACE}.svc.%s" $headless $domain -}}
{{- $parts := list (printf "CLIENT://%s:%v" $clientHost $clientPort) -}}
{{- if (include "fluss.listeners.external.enabled" $ctx) -}}
{{- $extName := include "fluss.listeners.external.name" $ctx -}}
{{- $extHost := include "fluss.listeners.external.hostExpr" (dict "ctx" $ctx "component" $component) -}}
{{- $extPort := include "fluss.listeners.external.portExpr" (dict "ctx" $ctx "component" $component) -}}
{{- $parts = append $parts (printf "%s://%s:%v" $extName $extHost $extPort) -}}
{{- end -}}
{{- join ", " $parts -}}
{{- end -}}

{{/*
Warning when tablet replicas > 1 would advertise a shared host:port, or when
EXTERNAL is PLAINTEXT.
Usage:
  include "fluss.listeners.validateWarning" .
*/}}
{{- define "fluss.listeners.validateWarning" -}}
{{- $msgs := list -}}
{{- if and (include "fluss.listeners.external.enabled" .) (eq (include "fluss.security.external.protocol" . | trim) "PLAINTEXT") -}}
{{- $msgs = append $msgs "listeners.external is enabled with PLAINTEXT; an unauthenticated listener is exposed outside the cluster. Set security.external.sasl.mechanism to plain or keep EXTERNAL on a trusted network." -}}
{{- end -}}
{{/* Only tablet is checked because the coordinator is single-replica today (no HA, FIP-9). */}}
{{- $replicas := .Values.tablet.numberOfReplicas | int -}}
{{- if and (include "fluss.listeners.external.enabled" .) (gt $replicas 1) -}}
{{- $hosts := include "fluss.listeners.external.hostsYaml" (dict "ctx" . "component" "tablet") | fromYamlArray -}}
{{- $ports := include "fluss.listeners.external.portsYaml" (dict "ctx" . "component" "tablet") | fromYamlArray -}}
{{- $uniqueHosts := and (eq (len $hosts) $replicas) (eq (len $hosts) (include "fluss.listeners.uniqueCount" $hosts | int)) -}}
{{- $uniquePorts := and (eq (len $ports) $replicas) (eq (len $ports) (include "fluss.listeners.uniqueCount" $ports | int)) -}}
{{- if not (or $uniqueHosts $uniquePorts) -}}
{{- $msgs = append $msgs (printf "listeners.external advertised host/port is the same for every tablet pod (tablet.numberOfReplicas is %d). Set tablet.listeners.external.advertisedHosts or advertisedPorts with one unique entry per replica." $replicas) -}}
{{- end -}}
{{- end -}}
{{- join "\n" $msgs -}}
{{- end -}}

{{/*
Number of unique stringified items in a list (YAML array via include arg).
Usage:
  include "fluss.listeners.uniqueCount" $list
*/}}
{{- define "fluss.listeners.uniqueCount" -}}
{{- $seen := dict -}}
{{- range $item := . -}}
{{- $_ := set $seen ($item | toString) true -}}
{{- end -}}
{{- len $seen -}}
{{- end -}}

{{/*
Listener configuration errors. Empty string when valid.
Usage:
  include "fluss.listeners.validateError" .
*/}}
{{- define "fluss.listeners.validateError" -}}
{{- $msgs := list -}}
{{- /* clusterDomain and override-key fights */ -}}
{{- $domain := include "fluss.listeners.clusterDomain" . -}}
{{- if eq $domain "" -}}
{{- $msgs = append $msgs "clusterDomain must not be empty" -}}
{{- end -}}
{{- if hasKey (.Values.configurationOverrides | default dict) "bind.listeners" -}}
{{- $msgs = append $msgs "configurationOverrides cannot set bind.listeners; the chart always writes it from listeners.* values" -}}
{{- end -}}
{{- if hasKey (.Values.configurationOverrides | default dict) "advertised.listeners" -}}
{{- $msgs = append $msgs "configurationOverrides cannot set advertised.listeners; the chart always writes it from listeners.* values" -}}
{{- end -}}
{{- /* coordinator is single-replica: no per-ordinal arrays */ -}}
{{- $coord := .Values.coordinator | default dict -}}
{{- $coordListeners := index $coord "listeners" | default dict -}}
{{- $coordExt := index $coordListeners "external" | default dict -}}
{{- $coordHosts := index $coordExt "advertisedHosts" | default list -}}
{{- $coordPorts := index $coordExt "advertisedPorts" | default list -}}
{{- if and (kindIs "slice" $coordHosts) (gt (len $coordHosts) 0) -}}
{{- $msgs = append $msgs "coordinator.listeners.external.advertisedHosts is not supported; the coordinator is single-replica, use advertisedHost" -}}
{{- end -}}
{{- if and (kindIs "slice" $coordPorts) (gt (len $coordPorts) 0) -}}
{{- $msgs = append $msgs "coordinator.listeners.external.advertisedPorts is not supported; the coordinator is single-replica, use advertisedPort" -}}
{{- end -}}
{{- if (include "fluss.listeners.external.enabled" .) -}}
{{- /* tablet arrays: type, mutual exclusion, length, uniqueness, tokens */ -}}
{{- $replicas := .Values.tablet.numberOfReplicas | int -}}
{{- $tablet := .Values.tablet | default dict -}}
{{- $tabletListeners := index $tablet "listeners" | default dict -}}
{{- $tabletExt := index $tabletListeners "external" | default dict -}}
{{- $rawHosts := index $tabletExt "advertisedHosts" | default list -}}
{{- $rawPorts := index $tabletExt "advertisedPorts" | default list -}}
{{- if and $rawHosts (not (kindIs "slice" $rawHosts)) -}}
{{- $msgs = append $msgs "tablet.listeners.external.advertisedHosts must be a list" -}}
{{- end -}}
{{- if and $rawPorts (not (kindIs "slice" $rawPorts)) -}}
{{- $msgs = append $msgs "tablet.listeners.external.advertisedPorts must be a list" -}}
{{- end -}}
{{- $hosts := include "fluss.listeners.external.hostsYaml" (dict "ctx" . "component" "tablet") | fromYamlArray -}}
{{- $ports := include "fluss.listeners.external.portsYaml" (dict "ctx" . "component" "tablet") | fromYamlArray -}}
{{- $tabletHost := index $tabletExt "advertisedHost" | default "" | toString | trim -}}
{{- $tabletPort := index $tabletExt "advertisedPort" | default "" | toString | trim -}}
{{- if and (gt (len $hosts) 0) $tabletHost -}}
{{- $msgs = append $msgs "tablet.listeners.external.advertisedHost and advertisedHosts cannot both be set" -}}
{{- end -}}
{{- if and (gt (len $ports) 0) $tabletPort -}}
{{- $msgs = append $msgs "tablet.listeners.external.advertisedPort and advertisedPorts cannot both be set" -}}
{{- end -}}
{{- if gt (len $hosts) 0 -}}
{{- if ne (len $hosts) $replicas -}}
{{- $msgs = append $msgs (printf "tablet.listeners.external.advertisedHosts length must equal tablet.numberOfReplicas (%d)" $replicas) -}}
{{- end -}}
{{- if ne (len $hosts) (include "fluss.listeners.uniqueCount" $hosts | int) -}}
{{- $msgs = append $msgs "tablet.listeners.external.advertisedHosts values must be unique" -}}
{{- end -}}
{{- range $i, $h := $hosts -}}
{{- $resolved := tpl ($h | toString) $ -}}
{{- if eq ($resolved | trim) "" -}}
{{- $msgs = append $msgs (printf "tablet.listeners.external.advertisedHosts[%d] must not be empty" $i) -}}
{{- else if include "fluss.listeners.invalidHostToken" $resolved -}}
{{- $msgs = append $msgs (printf "tablet.listeners.external.advertisedHosts[%d] must be a hostname/IP or ${NODE_IP} (shell formulas are not supported)" $i) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if gt (len $ports) 0 -}}
{{- if ne (len $ports) $replicas -}}
{{- $msgs = append $msgs (printf "tablet.listeners.external.advertisedPorts length must equal tablet.numberOfReplicas (%d)" $replicas) -}}
{{- end -}}
{{- if ne (len $ports) (include "fluss.listeners.uniqueCount" $ports | int) -}}
{{- $msgs = append $msgs "tablet.listeners.external.advertisedPorts values must be unique" -}}
{{- end -}}
{{- range $i, $p := $ports -}}
{{- if include "fluss.listeners.invalidPortToken" $p -}}
{{- $msgs = append $msgs (printf "tablet.listeners.external.advertisedPorts[%d] must be an integer" $i) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- /* required host/port and bind-port collision (including metrics) */ -}}
{{- $extHost := include "fluss.listeners.external.hostExpr" (dict "ctx" . "component" "tablet") -}}
{{- $coordHost := include "fluss.listeners.external.hostExpr" (dict "ctx" . "component" "coordinator") -}}
{{- if or (not $extHost) (not $coordHost) -}}
{{- $msgs = append $msgs "listeners.external.advertisedHost must be set when listeners.external.enabled is true (or tablet.listeners.external.advertisedHosts / coordinator.listeners.external.advertisedHost)" -}}
{{- end -}}
{{- $tabletScalarHost := include "fluss.listeners.external.scalar" (dict "ctx" . "component" "tablet" "field" "advertisedHost") -}}
{{- $coordScalarHost := include "fluss.listeners.external.scalar" (dict "ctx" . "component" "coordinator" "field" "advertisedHost") -}}
{{- if or (include "fluss.listeners.invalidHostToken" $tabletScalarHost) (include "fluss.listeners.invalidHostToken" $coordScalarHost) -}}
{{- $msgs = append $msgs "listeners.external.advertisedHost must be a hostname/IP or ${NODE_IP} (shell formulas are not supported)" -}}
{{- end -}}
{{- $tabletScalarPort := include "fluss.listeners.external.scalar" (dict "ctx" . "component" "tablet" "field" "advertisedPort") -}}
{{- $coordScalarPort := include "fluss.listeners.external.scalar" (dict "ctx" . "component" "coordinator" "field" "advertisedPort") -}}
{{- if or (include "fluss.listeners.invalidPortToken" $tabletScalarPort) (include "fluss.listeners.invalidPortToken" $coordScalarPort) -}}
{{- $msgs = append $msgs "listeners.external.advertisedPort must be an integer (shell formulas are not supported)" -}}
{{- end -}}
{{- $internalPort := .Values.listeners.internal.port | int -}}
{{- $clientPort := .Values.listeners.client.port | int -}}
{{- $externalPort := include "fluss.listeners.external.port" . | int -}}
{{- $promPort := (.Values.metrics.prometheus.port | default 9249) | int -}}
{{- $jmxPort := (.Values.metrics.jmx.port | default 9250) | int -}}
{{- if or (eq $externalPort $internalPort) (eq $externalPort $clientPort) (eq $externalPort $promPort) (eq $externalPort $jmxPort) -}}
{{- $msgs = append $msgs "listeners.external.port must differ from listeners.internal.port, listeners.client.port, metrics.prometheus.port, and metrics.jmx.port" -}}
{{- end -}}
{{- end -}}
{{- join "\n" $msgs -}}
{{- end -}}
