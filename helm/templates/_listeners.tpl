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
Fluss listener name for the optional EXTERNAL listener (bind/advertise/protocol map).
Usage:
  include "fluss.listeners.external.name" .
*/}}
{{- define "fluss.listeners.external.name" -}}
{{- $ext := .Values.listeners.external | default dict -}}
{{- $ext.name | default "EXTERNAL" | toString | trim -}}
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
Security protocol for the EXTERNAL listener. It always mirrors the client
listener because the EXTERNAL JAAS entry reuses the client SASL users, so an
independent protocol would have no credentials of its own.
Usage:
  include "fluss.listeners.external.protocol" .
*/}}
{{- define "fluss.listeners.external.protocol" -}}
{{- include "fluss.security.listener.protocol" (dict "context" .Values "listener" "client") | trim -}}
{{- end -}}

{{/*
Resolved advertised host or port for a component/kind after applying
per-component overrides and helm tpl (Release.Namespace and similar).
Empty means "use the chart default" (in-cluster FQDN for CLIENT host, bind
port for advertised ports).
Usage:
  include "fluss.listeners.rawAdvertisedValue" (dict "ctx" . "component" "tablet" "kind" "external" "field" "advertisedHost")
*/}}
{{- define "fluss.listeners.rawAdvertisedValue" -}}
{{- $ctx := .ctx -}}
{{- $comp := index $ctx.Values .component | default dict -}}
{{- $compListeners := index $comp "listeners" | default dict -}}
{{- $compKind := index $compListeners .kind | default dict -}}
{{- $globalKind := index $ctx.Values.listeners .kind | default dict -}}
{{- $compVal := index $compKind .field | default "" | toString | trim -}}
{{- $globalVal := index $globalKind .field | default "" | toString | trim -}}
{{- $raw := $compVal | default $globalVal -}}
{{- if $raw -}}
{{- tpl $raw $ctx | trim -}}
{{- end -}}
{{- end -}}

{{/*
Advertised port for a component listener. Falls back to the bind port.
Usage:
  include "fluss.listeners.advertisedPort" (dict "ctx" . "component" "tablet" "kind" "client")
*/}}
{{- define "fluss.listeners.advertisedPort" -}}
{{- $raw := include "fluss.listeners.rawAdvertisedValue" (dict "ctx" .ctx "component" .component "kind" .kind "field" "advertisedPort") -}}
{{- if $raw -}}
{{- $raw -}}
{{- else if eq .kind "client" -}}
{{- .ctx.Values.listeners.client.port -}}
{{- else -}}
{{- include "fluss.listeners.external.port" .ctx -}}
{{- end -}}
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
Pass the chart root as ctx and the component name so each StatefulSet can use
its own headless Service DNS and optional overrides.
Usage:
  include "fluss.listeners.advertised" (dict "ctx" . "component" "tablet")
*/}}
{{- define "fluss.listeners.advertised" -}}
{{- $ctx := .ctx -}}
{{- $component := .component -}}
{{- $headless := ternary "tablet-server-hs" "coordinator-server-hs" (eq $component "tablet") -}}
{{- $clientHost := include "fluss.listeners.rawAdvertisedValue" (dict "ctx" $ctx "component" $component "kind" "client" "field" "advertisedHost") -}}
{{- if not $clientHost -}}
{{- $clientHost = printf "${POD_NAME}.%s.${POD_NAMESPACE}.svc.cluster.local" $headless -}}
{{- end -}}
{{- $clientPort := include "fluss.listeners.advertisedPort" (dict "ctx" $ctx "component" $component "kind" "client") -}}
{{- $parts := list (printf "CLIENT://%s:%v" $clientHost $clientPort) -}}
{{- if (include "fluss.listeners.external.enabled" $ctx) -}}
{{- $extName := include "fluss.listeners.external.name" $ctx -}}
{{- $extHost := include "fluss.listeners.rawAdvertisedValue" (dict "ctx" $ctx "component" $component "kind" "external" "field" "advertisedHost") -}}
{{- $extPort := include "fluss.listeners.advertisedPort" (dict "ctx" $ctx "component" $component "kind" "external") -}}
{{- $parts = append $parts (printf "%s://%s:%v" $extName $extHost $extPort) -}}
{{- end -}}
{{- join ", " $parts -}}
{{- end -}}

{{/*
True when an expression references a specific shell env token, including
expansion forms like ${TOKEN##*-}.
Usage:
  include "fluss.listeners.usesEnvToken" (dict "blob" $expr "token" "POD_NAME")
*/}}
{{- define "fluss.listeners.usesEnvToken" -}}
{{- $blob := .blob | toString -}}
{{- $token := .token | toString -}}
{{- if or (contains (printf "$%s" $token) $blob) (contains (printf "${%s" $token) $blob) -}}true{{- end -}}
{{- end -}}

{{/*
True when a component's advertised listeners reference the Kubernetes node IP,
so the StatefulSet needs the NODE_IP env from the Downward API. Only added when
referenced so default deployments keep an unchanged pod spec.
Usage:
  include "fluss.listeners.usesNodeIp" (dict "ctx" . "component" "tablet")
*/}}
{{- define "fluss.listeners.usesNodeIp" -}}
{{- $adv := include "fluss.listeners.advertised" (dict "ctx" .ctx "component" .component) -}}
{{- if (include "fluss.listeners.usesEnvToken" (dict "blob" $adv "token" "NODE_IP")) -}}true{{- end -}}
{{- end -}}

{{/*
True when a host:port pair embeds a per-pod identity token so replicas do not
all advertise the same endpoint.
Usage:
  include "fluss.listeners.hasPerPodIdentity" (dict "host" $host "port" $port)
*/}}
{{- define "fluss.listeners.hasPerPodIdentity" -}}
{{- $blob := printf "%s:%s" (.host | toString) (.port | toString) -}}
{{- if or
    (include "fluss.listeners.usesEnvToken" (dict "blob" $blob "token" "POD_NAME"))
    (include "fluss.listeners.usesEnvToken" (dict "blob" $blob "token" "POD_IP"))
    (include "fluss.listeners.usesEnvToken" (dict "blob" $blob "token" "NODE_IP"))
-}}
true
{{- end -}}
{{- end -}}

{{/*
Warning when tablet replicas > 1 would advertise a shared host:port.
Usage:
  include "fluss.listeners.validateWarning" .
*/}}
{{- define "fluss.listeners.validateWarning" -}}
{{/* Only tablet is checked because the coordinator is single-replica today (no HA, FIP-9). */}}
{{- $replicas := .Values.tablet.numberOfReplicas | int -}}
{{- if le $replicas 1 -}}
{{- else -}}
{{- $msgs := list -}}
{{- $clientHost := include "fluss.listeners.rawAdvertisedValue" (dict "ctx" . "component" "tablet" "kind" "client" "field" "advertisedHost") -}}
{{- if $clientHost -}}
{{- $clientPort := include "fluss.listeners.advertisedPort" (dict "ctx" . "component" "tablet" "kind" "client") | toString -}}
{{- if not (include "fluss.listeners.hasPerPodIdentity" (dict "host" $clientHost "port" $clientPort)) -}}
{{- $msgs = append $msgs (printf "listeners.client advertised host/port is the same for every tablet pod (tablet.numberOfReplicas is %d). Include $POD_NAME, $POD_IP, $NODE_IP, or ${POD_NAME##*-} in the host or port so each pod advertises a unique endpoint." $replicas) -}}
{{- end -}}
{{- end -}}
{{- if (include "fluss.listeners.external.enabled" .) -}}
{{- $extHost := include "fluss.listeners.rawAdvertisedValue" (dict "ctx" . "component" "tablet" "kind" "external" "field" "advertisedHost") -}}
{{- $extPort := include "fluss.listeners.advertisedPort" (dict "ctx" . "component" "tablet" "kind" "external") | toString -}}
{{- if and $extHost (not (include "fluss.listeners.hasPerPodIdentity" (dict "host" $extHost "port" $extPort))) -}}
{{- $msgs = append $msgs (printf "listeners.external advertised host/port is the same for every tablet pod (tablet.numberOfReplicas is %d). Include $POD_NAME, $POD_IP, $NODE_IP, or ${POD_NAME##*-} in the host or port so each pod advertises a unique endpoint." $replicas) -}}
{{- end -}}
{{- end -}}
{{- join "\n" $msgs -}}
{{- end -}}
{{- end -}}

{{/*
Listener configuration errors. Empty string when valid.
Usage:
  include "fluss.listeners.validateError" .
*/}}
{{- define "fluss.listeners.validateError" -}}
{{- $msgs := list -}}
{{- if hasKey (.Values.configurationOverrides | default dict) "bind.listeners" -}}
{{- $msgs = append $msgs "configurationOverrides cannot set bind.listeners; the chart always writes it from listeners.* values" -}}
{{- end -}}
{{- if hasKey (.Values.configurationOverrides | default dict) "advertised.listeners" -}}
{{- $msgs = append $msgs "configurationOverrides cannot set advertised.listeners; the chart always writes it from listeners.* values" -}}
{{- end -}}
{{- if (include "fluss.listeners.external.enabled" .) -}}
{{- $name := include "fluss.listeners.external.name" . -}}
{{- if eq $name "" -}}
{{- $msgs = append $msgs "listeners.external.name must not be empty when listeners.external.enabled is true" -}}
{{- end -}}
{{- if has (upper $name) (list "INTERNAL" "CLIENT") -}}
{{- $msgs = append $msgs "listeners.external.name must not be INTERNAL or CLIENT" -}}
{{- end -}}
{{- $extHost := include "fluss.listeners.rawAdvertisedValue" (dict "ctx" . "component" "tablet" "kind" "external" "field" "advertisedHost") -}}
{{- $coordHost := include "fluss.listeners.rawAdvertisedValue" (dict "ctx" . "component" "coordinator" "kind" "external" "field" "advertisedHost") -}}
{{- if or (not $extHost) (not $coordHost) -}}
{{- $msgs = append $msgs "listeners.external.advertisedHost must be set when listeners.external.enabled is true" -}}
{{- end -}}
{{- $internalPort := .Values.listeners.internal.port | int -}}
{{- $clientPort := .Values.listeners.client.port | int -}}
{{- $externalPort := include "fluss.listeners.external.port" . | int -}}
{{- if or (eq $externalPort $internalPort) (eq $externalPort $clientPort) -}}
{{- $msgs = append $msgs "listeners.external.port must differ from listeners.internal.port and listeners.client.port" -}}
{{- end -}}
{{- end -}}
{{- join "\n" $msgs -}}
{{- end -}}
