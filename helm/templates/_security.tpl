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
Returns the authentication mechanism value of a given listener.
Allowed mechanism values: 'none', 'plain'
Usage:
  include "fluss.security.listener.mechanism" (dict "context" .Values "listener" "client")
*/}}
{{- define "fluss.security.listener.mechanism" -}}
{{- $listener := index .context.security .listener | default (dict) -}}
{{- $sasl := $listener.sasl | default (dict) -}}
{{- $mechanism := lower (default "none" $sasl.mechanism) -}}
{{- if and (ne $mechanism "") (not (has $mechanism (list "none" "plain"))) -}}
{{- fail (printf "security.%s.sasl.mechanism must be one of: none, plain" .listener) -}}
{{- end -}}
{{- if eq $mechanism "" -}}none{{- else -}}{{- $mechanism -}}{{- end -}}
{{- end -}}

{{/*
Returns true if any of the listeners uses SASL based authentication mechanism ('plain' for now).
Usage:
  include "fluss.security.sasl.enabled" .
*/}}
{{- define "fluss.security.sasl.enabled" -}}
{{- $internal := include "fluss.security.listener.mechanism" (dict "context" .Values "listener" "internal") -}}
{{- $client := include "fluss.security.listener.mechanism" (dict "context" .Values "listener" "client") -}}
{{- if or (ne $internal "none") (ne $client "none") -}}true{{- end -}}
{{- end -}}

{{/*
Returns true if any of the listeners uses 'plain' authentication mechanism.
Usage:
  include "fluss.security.sasl.plain.enabled" .
*/}}
{{- define "fluss.security.sasl.plain.enabled" -}}
{{- $internal := include "fluss.security.listener.mechanism" (dict "context" .Values "listener" "internal") -}}
{{- $client := include "fluss.security.listener.mechanism" (dict "context" .Values "listener" "client") -}}
{{- if or (eq $internal "plain") (eq $client "plain") -}}true{{- end -}}
{{- end -}}

{{/*
Returns protocol value derived from listener mechanism.
Usage:
  include "fluss.security.listener.protocol" (dict "context" .Values "listener" "internal")
*/}}
{{- define "fluss.security.listener.protocol" -}}
{{- $mechanism := include "fluss.security.listener.mechanism" (dict "context" .context "listener" .listener) -}}
{{- if eq $mechanism "none" -}}PLAINTEXT{{- else -}}SASL{{- end -}}
{{- end -}}

{{/*
Returns comma separated list of enabled mechanisms.
Usage:
  include "fluss.security.sasl.enabledMechanisms" .
*/}}
{{- define "fluss.security.sasl.enabledMechanisms" -}}
{{- $mechanisms := list -}}
{{- range $listener := list "internal" "client" -}}
  {{- $current := include "fluss.security.listener.mechanism" (dict "context" $.Values "listener" $listener) -}}
  {{- if and (ne $current "none") (not (has (upper $current) $mechanisms)) -}}
    {{- $mechanisms = append $mechanisms (upper $current) -}}
  {{- end -}}
{{- end -}}
{{- join "," $mechanisms -}}
{{- end -}}

{{/*
Validates that the client PLAIN mechanism block contains the required users.
Usage:
  include "fluss.security.sasl.validateClientPlainUsers" .
*/}}
{{- define "fluss.security.sasl.validateClientPlainUsers" -}}
{{- $clientMechanism := include "fluss.security.listener.mechanism" (dict "context" .Values "listener" "client") -}}

{{- if eq $clientMechanism "plain" -}}
  {{- $users := .Values.security.client.sasl.plain.users | default (list) -}}
  {{- if eq (len $users) 0 -}}
  {{- fail "security.client.sasl.plain.users must contain at least one user when security.client.sasl.mechanism is plain" -}}
  {{- end -}}
  {{- range $idx, $user := $users -}}
    {{- if or (empty $user.username) (empty $user.password) -}}
    {{- fail (printf "security.client.sasl.plain.users[%d] must set both username and password" $idx) -}}
    {{- end -}}
  {{- end -}}
{{- end -}}
{{- end -}}

{{/*
Returns the SASL JAAS config name.
Usage:
  include "fluss.security.sasl.configName" .
*/}}
{{- define "fluss.security.sasl.configName" -}}
{{ include "fluss.fullname" . }}-sasl-jaas-config
{{- end -}}
