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
Returns the ZooKeeper SASL authentication mechanism value.
Allowed mechanism values: '', 'plain'
Usage:
  include "fluss.security.zookeeper.sasl.mechanism" .
*/}}
{{- define "fluss.security.zookeeper.sasl.mechanism" -}}
{{- $sasl := .Values.security.zookeeper.sasl | default (dict) -}}
{{- $mechanism := lower (default "" $sasl.mechanism) -}}
{{- $mechanism -}}
{{- end -}}

{{/*
Returns true if ZooKeeper SASL authentication is enabled (mechanism is non-empty).
Usage:
  include "fluss.security.zookeeper.sasl.enabled" .
*/}}
{{- define "fluss.security.zookeeper.sasl.enabled" -}}
{{- $mechanism := include "fluss.security.zookeeper.sasl.mechanism" . -}}
{{- if ne $mechanism "" -}}true{{- end -}}
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
Validates security.client.sasl.plain.users. Each entry is either a literal
{username, password} pair OR {existingSecret: {name, usernameKey?, passwordKey?}}.
Mixing the two shapes within one entry is not allowed.
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
    {{- $errs := list -}}
    {{- range $idx, $user := $users -}}
      {{- $ref := $user.existingSecret | default (dict) -}}
      {{- $hasLiteral := or (not (empty $user.username)) (not (empty $user.password)) -}}
      {{- $hasRef := not (empty $ref.name) -}}
      {{- if and $hasLiteral $hasRef -}}
        {{- $errs = append $errs (printf "security.client.sasl.plain.users[%d] cannot set username/password and existingSecret" $idx) -}}
      {{- else if $hasRef -}}
        {{/* existingSecret path — name is the only required field */}}
      {{- else -}}
        {{- if or (empty $user.username) (empty $user.password) -}}
          {{- $errs = append $errs (printf "security.client.sasl.plain.users[%d] must set both username and password (or existingSecret)" $idx) -}}
        {{- end -}}
      {{- end -}}
    {{- end -}}
    {{- join "\n" $errs -}}
  {{- end -}}
{{- end -}}
{{- end -}}

{{/*
Validates that ZooKeeper SASL mechanism is valid.
Returns an error message if invalid, empty string otherwise.
Usage:
  include "fluss.security.zookeeper.sasl.validateMechanism" .
*/}}
{{- define "fluss.security.zookeeper.sasl.validateMechanism" -}}
{{- $allowedMechanisms := list "" "plain" -}}
{{- $mechanism := include "fluss.security.zookeeper.sasl.mechanism" . -}}
{{- if not (has $mechanism $allowedMechanisms) -}}
  {{- print "security.zookeeper.sasl.mechanism must be empty or: plain" -}}
{{- end -}}
{{- end -}}

{{/*
Validates that ZooKeeper SASL loginModuleClass is not empty when ZK SASL is enabled.
Returns an error message if invalid, empty string otherwise.
Usage:
  include "fluss.security.zookeeper.sasl.validateLoginModuleClass" .
*/}}
{{- define "fluss.security.zookeeper.sasl.validateLoginModuleClass" -}}
{{- if and (include "fluss.security.zookeeper.sasl.enabled" .) (not .Values.security.zookeeper.sasl.plain.loginModuleClass) -}}
  {{- print "security.zookeeper.sasl.plain.loginModuleClass must not be empty when security.zookeeper.sasl.mechanism is plain" -}}
{{- end -}}
{{- end -}}

{{/*
Validates that ZooKeeper SASL username is provided when ZK SASL is enabled,
either as a literal or via existingSecret.
Returns an error message if invalid, empty string otherwise.
Usage:
  include "fluss.security.zookeeper.sasl.validateUsername" .
*/}}
{{- define "fluss.security.zookeeper.sasl.validateUsername" -}}
{{- if include "fluss.security.zookeeper.sasl.enabled" . -}}
  {{- if not (include "fluss.security.sasl.plain.zookeeper.fromSecret" .) -}}
    {{- if not .Values.security.zookeeper.sasl.plain.username -}}
      {{- print "security.zookeeper.sasl.plain.username must not be empty when security.zookeeper.sasl.mechanism is plain (or set security.zookeeper.sasl.plain.existingSecret)" -}}
    {{- end -}}
  {{- end -}}
{{- end -}}
{{- end -}}

{{/*
Validates that ZooKeeper SASL password is provided when ZK SASL is enabled,
either as a literal or via existingSecret.
Returns an error message if invalid, empty string otherwise.
Usage:
  include "fluss.security.zookeeper.sasl.validatePassword" .
*/}}
{{- define "fluss.security.zookeeper.sasl.validatePassword" -}}
{{- if include "fluss.security.zookeeper.sasl.enabled" . -}}
  {{- if not (include "fluss.security.sasl.plain.zookeeper.fromSecret" .) -}}
    {{- if not .Values.security.zookeeper.sasl.plain.password -}}
      {{- print "security.zookeeper.sasl.plain.password must not be empty when security.zookeeper.sasl.mechanism is plain (or set security.zookeeper.sasl.plain.existingSecret)" -}}
    {{- end -}}
  {{- end -}}
{{- end -}}
{{- end -}}

{{/*
Returns the default internal SASL username based on the release name.
Usage:
  include "fluss.security.sasl.plain.internal.defaultUsername" .
*/}}
{{- define "fluss.security.sasl.plain.internal.defaultUsername" -}}
{{- printf "fluss-internal-user-%s" .Release.Name -}}
{{- end -}}

{{/*
Returns the default internal SASL password based on the release name (sha256 hashed).
Usage:
  include "fluss.security.sasl.plain.internal.defaultPassword" .
*/}}
{{- define "fluss.security.sasl.plain.internal.defaultPassword" -}}
{{- printf "fluss-internal-password-%s" .Release.Name | sha256sum -}}
{{- end -}}

{{/*
Returns the resolved internal SASL username (user-provided or auto-generated default).
Usage:
  include "fluss.security.sasl.plain.internal.username" .
*/}}
{{- define "fluss.security.sasl.plain.internal.username" -}}
{{- .Values.security.internal.sasl.plain.username | default (include "fluss.security.sasl.plain.internal.defaultUsername" .) -}}
{{- end -}}

{{/*
Returns the resolved internal SASL password (user-provided or auto-generated default).
Usage:
  include "fluss.security.sasl.plain.internal.password" .
*/}}
{{- define "fluss.security.sasl.plain.internal.password" -}}
{{- .Values.security.internal.sasl.plain.password | default (include "fluss.security.sasl.plain.internal.defaultPassword" .) -}}
{{- end -}}

{{/*
Returns true if JAAS configuration is required, either by listeners using SASL protocol or ZooKeeper SASL enablement.
Usage:
  include "fluss.security.jaas.required" .
*/}}
{{- define "fluss.security.jaas.required" -}}
{{- if or (include "fluss.security.sasl.enabled" .) (include "fluss.security.zookeeper.sasl.enabled" .) -}}
{{- true -}}
{{- end -}}
{{- end -}}

{{/*
Returns a warning if the internal SASL user is using auto-generated credentials.
Usage:
  include "fluss.security.sasl.warnInternalUser" .
*/}}
{{- define "fluss.security.sasl.warnInternalUser" -}}
{{- if (include "fluss.security.sasl.enabled" .) -}}
  {{- $internalMechanism := include "fluss.security.listener.mechanism" (dict "context" .Values "listener" "internal") -}}
  {{- if eq $internalMechanism "plain" -}}
    {{- if not (include "fluss.security.sasl.plain.internal.fromSecret" .) -}}
      {{- if and (not .Values.security.internal.sasl.plain.username) (not .Values.security.internal.sasl.plain.password) -}}
        {{- print "You are using AUTO-GENERATED SASL credentials for internal communication.\n  It is strongly recommended to set the following values in production:\n    - security.internal.sasl.plain.username\n    - security.internal.sasl.plain.password\n  Or source from an existing Secret via security.internal.sasl.plain.existingSecret" -}}
      {{- end -}}
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
{{- $errMessages = append $errMessages (include "fluss.security.zookeeper.sasl.validateMechanism" .) -}}
{{- $errMessages = append $errMessages (include "fluss.security.zookeeper.sasl.validateLoginModuleClass" .) -}}
{{- $errMessages = append $errMessages (include "fluss.security.zookeeper.sasl.validateUsername" .) -}}
{{- $errMessages = append $errMessages (include "fluss.security.zookeeper.sasl.validatePassword" .) -}}

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
  include "fluss.security.jaas.configName" .
*/}}
{{- define "fluss.security.jaas.configName" -}}
{{ include "fluss.fullname" . }}-sasl-jaas-config
{{- end -}}

{{/*
Returns "true" if internal SASL credentials come from an existingSecret.
Usage:
  include "fluss.security.sasl.plain.internal.fromSecret" .
*/}}
{{- define "fluss.security.sasl.plain.internal.fromSecret" -}}
{{- $ref := .Values.security.internal.sasl.plain.existingSecret | default (dict) -}}
{{- if $ref.name -}}true{{- end -}}
{{- end -}}

{{/*
Returns "true" if ZooKeeper SASL credentials come from an existingSecret.
Usage:
  include "fluss.security.sasl.plain.zookeeper.fromSecret" .
*/}}
{{- define "fluss.security.sasl.plain.zookeeper.fromSecret" -}}
{{- $ref := .Values.security.zookeeper.sasl.plain.existingSecret | default (dict) -}}
{{- if $ref.name -}}true{{- end -}}
{{- end -}}

{{/*
Returns the env-var name for an internal SASL credential field.
Usage:
  include "fluss.security.sasl.plain.internal.envVarName" "username"
  include "fluss.security.sasl.plain.internal.envVarName" "password"
*/}}
{{- define "fluss.security.sasl.plain.internal.envVarName" -}}
{{- printf "FLUSS_JAAS_INTERNAL_%s" (upper .) -}}
{{- end -}}

{{/*
Returns the env-var name for a ZooKeeper SASL credential field.
Usage:
  include "fluss.security.sasl.plain.zookeeper.envVarName" "username"
  include "fluss.security.sasl.plain.zookeeper.envVarName" "password"
*/}}
{{- define "fluss.security.sasl.plain.zookeeper.envVarName" -}}
{{- printf "FLUSS_JAAS_ZOOKEEPER_%s" (upper .) -}}
{{- end -}}

{{/*
Returns the env-var name for a client user credential field at a given index.
Usage:
  include "fluss.security.sasl.plain.client.envVarName" (dict "field" "username" "idx" 2)
  =>  FLUSS_JAAS_CLIENT_USERNAME_2
*/}}
{{- define "fluss.security.sasl.plain.client.envVarName" -}}
{{- printf "FLUSS_JAAS_CLIENT_%s_%d" (upper .field) (int .idx) -}}
{{- end -}}

