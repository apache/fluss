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
Allowed mechanism values: '', 'plain'
Usage:
  include "fluss.security.listener.mechanism" (dict "context" .Values "listener" "client")
*/}}
{{- define "fluss.security.listener.mechanism" -}}
{{- $listener := index .context.security .listener | default (dict) -}}
{{- $sasl := $listener.sasl | default (dict) -}}
{{- $mechanism := lower (default "" $sasl.mechanism) -}}
{{- $mechanism -}}
{{- end -}}

{{/*
Returns true if any of the listeners uses SASL based authentication mechanism ('plain' for now).
Usage:
  include "fluss.security.sasl.enabled" .
*/}}
{{- define "fluss.security.sasl.enabled" -}}
{{- $internal := include "fluss.security.listener.mechanism" (dict "context" .Values "listener" "internal") -}}
{{- $client := include "fluss.security.listener.mechanism" (dict "context" .Values "listener" "client") -}}
{{- if or (ne $internal "") (ne $client "") -}}true{{- end -}}
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
{{- if eq $mechanism "" -}}PLAINTEXT{{- else -}}SASL{{- end -}}
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
  {{- if and (ne $current "") (not (has (upper $current) $mechanisms)) -}}
    {{- $mechanisms = append $mechanisms (upper $current) -}}
  {{- end -}}
{{- end -}}
{{- join "," $mechanisms -}}
{{- end -}}

{{/*
Validates that SASL mechanisms are valid.
Returns an error message if invalid, empty string otherwise.
Usage:
  include "fluss.security.sasl.validateMechanisms" .
*/}}
{{- define "fluss.security.sasl.validateMechanisms" -}}
{{- $allowedMechanisms := list "" "plain" -}}
{{- range $listener := list "internal" "client" -}}
  {{- $listenerValues := index $.Values.security $listener | default (dict) -}}
  {{- $sasl := $listenerValues.sasl | default (dict) -}}
  {{- $mechanism := lower (default "" $sasl.mechanism) -}}
  {{- if not (has $mechanism $allowedMechanisms) -}}
    {{- printf "security.%s.sasl.mechanism must be empty or: plain" $listener -}}
  {{- end -}}
{{- end -}}
{{- end -}}

{{/*
Validates that the client PLAIN mechanism block contains the required users.
Returns an error message if invalid, empty string otherwise.
Usage:
  include "fluss.security.sasl.validateClientPlainUsers" .
*/}}
{{- define "fluss.security.sasl.validateClientPlainUsers" -}}
{{- $clientMechanism := include "fluss.security.listener.mechanism" (dict "context" .Values "listener" "client") -}}
{{- if eq $clientMechanism "plain" -}}
  {{- $users := .Values.security.client.sasl.plain.users | default (list) -}}
  {{- if eq (len $users) 0 -}}
    {{- print "security.client.sasl.plain.users must contain at least one user when security.client.sasl.mechanism is plain" -}}
  {{- else -}}
    {{- range $idx, $user := $users -}}
      {{- if or (empty $user.username) (empty $user.password) -}}
        {{- printf "security.client.sasl.plain.users[%d] must set both username and password" $idx -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}
{{- end -}}
{{- end -}}

{{/*
Returns a warning if the internal SASL user is using default credentials.
Usage:
  include "fluss.security.sasl.warnInternalUser" .
*/}}
{{- define "fluss.security.sasl.warnInternalUser" -}}
{{- if (include "fluss.security.sasl.enabled" .) -}}
  {{- $internalMechanism := include "fluss.security.listener.mechanism" (dict "context" .Values "listener" "internal") -}}
  {{- if eq $internalMechanism "plain" -}}
    {{- if and (eq .Values.security.internal.sasl.plain.username "fluss-internal-user") (eq .Values.security.internal.sasl.plain.password "fluss-internal-password") -}}
      {{- print "You are using the DEFAULT SASL credentials for internal communication.\n  It is strongly recommended to override the following values in production:\n    - security.internal.sasl.plain.username\n    - security.internal.sasl.plain.password" -}}
    {{- end -}}
  {{- end -}}
{{- end -}}
{{- end -}}

{{/*
Compile all warnings and errors into a single message.
Usage:
  include "fluss.security.validateValues" .
*/}}
{{- define "fluss.security.validateValues" -}}

{{- $errMessages := list -}}
{{- $errMessages = append $errMessages (include "fluss.security.sasl.validateMechanisms" .) -}}
{{- $errMessages = append $errMessages (include "fluss.security.sasl.validateClientPlainUsers" .) -}}

{{- $errMessages = without $errMessages "" -}}
{{- $errMessage := join "\n" $errMessages -}}

{{- $warnMessages := list -}}
{{- $warnMessages = append $warnMessages (include "fluss.security.sasl.warnInternalUser" .) -}}

{{- $warnMessages = without $warnMessages "" -}}
{{- $warnMessage := join "\n" $warnMessages -}}

{{- if $warnMessage -}}
{{-   printf "\nVALUES WARNING:\n%s" $warnMessage -}}
{{- end -}}

{{- if $errMessage -}}
{{-   printf "\nVALUES VALIDATION:\n%s" $errMessage | fail -}}
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
