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
Allowed mechanism values: 'none', 'plain', 'scram', 'oauthbearer'
Usage:
  include "fluss.security.listener.mechanism" (dict "Values" .Values "listener" "client")

Explanation:
(dict "Values" .Values "listener" "client") is a Helm template map, literally passed as a single argument to include.

- dict: creates a key value map
- "Values": is a key, .Values is its value
- "listener": is a key, "client" is its value

So this builds an object like:
{
  Values: .Values,
  listener: "client"
}

Inside the called helper, in this case, 'fluss.security.listener.mechanism', it is accessed as:
- .Values -> the chart values
- .listener -> "client"

The reason for this is that include can only pass one argument, so dict is a standard way to pass multiple named inputs.
*/}}
{{- define "fluss.security.listener.mechanism" -}}
{{- $listener := index .Values.security .listener | default (dict) -}}
{{- $sasl := $listener.sasl | default (dict) -}}
{{- $mechanism := lower (default "" $sasl.mechanism) -}}
{{- if not (has $mechanism (list "none" "plain" "scram" "oauthbearer")) -}}
{{- fail (printf "security.%s.sasl.mechanism must be one of: none, plain, scram, oauthbearer" .listener) -}}
{{- end -}}
{{- $mechanism -}}
{{- end -}}

{{/*
Returns true if any of the listeners uses SASL based authentication mechanism ('plain', 'scram', 'oauthbearer').
Usage:
  include "fluss.security.sasl.enabled" .
*/}}
{{- define "fluss.security.sasl.enabled" -}}
{{- $internal := include "fluss.security.listener.mechanism" (dict "Values" .Values "listener" "internal") -}}
{{- $client := include "fluss.security.listener.mechanism" (dict "Values" .Values "listener" "client") -}}
{{- if or (ne $internal "none") (ne $client "none") -}}true{{- end -}}
{{- end -}}

{{/*
Returns true if any of the listeners uses 'plain' authentication mechanism.
Usage:
  include "fluss.security.sasl.plain.enabled" .
*/}}
{{- define "fluss.security.sasl.plain.enabled" -}}
{{- $internal := include "fluss.security.listener.mechanism" (dict "Values" .Values "listener" "internal") -}}
{{- $client := include "fluss.security.listener.mechanism" (dict "Values" .Values "listener" "client") -}}
{{- if or (eq $internal "plain") (eq $client "plain") -}}true{{- end -}}
{{- end -}}

{{/*
Returns protocol value derived from listener mechanism.
Usage:
  include "fluss.security.listener.protocol" (dict "Values" .Values "listener" "internal")
*/}}
{{- define "fluss.security.listener.protocol" -}}
{{- $mechanism := include "fluss.security.listener.mechanism" (dict "Values" .Values "listener" .listener) -}}
{{- if eq $mechanism "none" -}}PLAINTEXT{{- else -}}SASL{{- end -}}
{{- end -}}

{{/*
Returns values file mechanisms as the Fluss expected configuration mechanisms.

Why this helper exists:
- Values file use: none | plain | scram | oauthbearer
- Fluss configuration expects: PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512 | OAUTHBEARER
- SCRAM also needs sha variant mapping/validation (256 or 512)

This is internal method, specific to this file used by other methods.

Usage:
  include "fluss.security.listener.normalizeMechanism" (dict "Values" .Values "listener" "client")
*/}}
{{- define "fluss.security.listener.normalizeMechanism" -}}
{{- $mechanism := include "fluss.security.listener.mechanism" (dict "Values" .Values "listener" .listener) -}}
{{- if eq $mechanism "plain" -}}
PLAIN
{{- else if eq $mechanism "oauthbearer" -}}
OAUTHBEARER
{{- else if eq $mechanism "scram" -}}
  {{- $listenerBlock := index .Values.security .listener | default (dict) -}}
  {{- $sasl := $listenerBlock.sasl | default (dict) -}}
  {{- $scram := $sasl.scram | default (dict) -}}
  {{- $sha := int (default 256 $scram.sha) -}}
  {{- if and (ne $sha 256) (ne $sha 512) -}}
  {{- fail (printf "security.%s.sasl.scram.sha must be 256 or 512" .listener) -}}
  {{- end -}}
SCRAM-SHA-{{ $sha }}
{{- else -}}
NONE
{{- end -}}
{{- end -}}

{{/*
Returns comma separated list of enabled mechanisms.
Usage:
  include "fluss.security.sasl.enabledMechanisms" .

Example usage:
  echo "security.sasl.enabled.mechanisms: {{ include "fluss.security.sasl.enabledMechanisms" . | trim }}"
*/}}
{{- define "fluss.security.sasl.enabledMechanisms" -}}
{{- $mechanisms := list -}}
{{- range $listener := list "internal" "client" -}}
  {{- $current := include "fluss.security.listener.normalizeMechanism" (dict "Values" $.Values "listener" $listener) | trim -}}
  {{- if and (ne $current "") (ne $current "NONE") (not (has $current $mechanisms)) -}}
    {{- $mechanisms = append $mechanisms $current -}}
  {{- end -}}
{{- end -}}
{{- join "," $mechanisms -}}
{{- end -}}

{{/*
Returns the normalized client listener mechanism.
Usage:
  include "fluss.security.internal.clientMechanism" .

Example usage:
  "client.security.sasl.mechanism: {{ include "fluss.security.internal.clientMechanism" . | trim }}"
*/}}
{{- define "fluss.security.internal.clientMechanism" -}}
{{- include "fluss.security.listener.normalizeMechanism" (dict "Values" .Values "listener" "internal") | trim -}}
{{- end -}}

{{/*
Validates that the PLAIN mechanism block contains the required users.

Usage:
  include "fluss.security.sasl.validatePlainUsers" .
*/}}
{{- define "fluss.security.sasl.validatePlainUsers" -}}
{{- $clientMechanism := include "fluss.security.listener.mechanism" (dict "Values" .Values "listener" "client") -}}
{{- $internalMechanism := include "fluss.security.listener.mechanism" (dict "Values" .Values "listener" "internal") -}}

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

{{- if eq $internalMechanism "plain" -}}
  {{- if or (empty .Values.security.internal.sasl.plain.username) (empty .Values.security.internal.sasl.plain.password) -}}
  {{- fail "security.internal.sasl.plain.username and security.internal.sasl.plain.password are required when security.internal.sasl.mechanism is plain" -}}
  {{- end -}}
{{- end -}}
{{- end -}}

{{/*
Renders security configuration lines that are appended to the server.yaml file.
Usage:
  include "fluss.security.renderSecurityOptions" .
*/}}
{{- define "fluss.security.renderSecurityOptions" -}}
{{- $internalProtocol := include "fluss.security.listener.protocol" (dict "Values" .Values "listener" "internal") | trim -}}
{{- $enabledMechanisms := include "fluss.security.sasl.enabledMechanisms" . | trim }}
{{- $internalClientMechanism := include "fluss.security.internal.clientMechanism" . | trim }}

{{- if (include "fluss.security.sasl.enabled" .) }}
echo "security.sasl.enabled.mechanisms: {{ $enabledMechanisms }}" >> $FLUSS_HOME/conf/server.yaml && \
{{- if eq $internalProtocol "SASL" }}
echo "client.security.protocol: SASL" >> $FLUSS_HOME/conf/server.yaml && \
echo "client.security.sasl.mechanism: {{ $internalClientMechanism }}" >> $FLUSS_HOME/conf/server.yaml && \
{{- end }}

{{- if (include "fluss.security.sasl.plain.enabled" .) }}
export FLUSS_ENV_JAVA_OPTS="-Djava.security.auth.login.config=/etc/fluss/conf/jaas.conf ${FLUSS_ENV_JAVA_OPTS}" && \
{{- end }}
{{- end }}

{{- end -}}
