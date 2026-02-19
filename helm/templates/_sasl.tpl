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
Return true if SASL is configured in any of the listener protocols
*/}}
{{- define "fluss.sasl.enabled" -}}
{{- $enabled := false -}}
{{- range $id, $l := .Values.listeners -}}
  {{- if and (not $enabled) (regexFind "SASL" (upper $l.protocol)) -}}
    {{- $enabled = true -}}
  {{- end -}}
{{- end -}}
{{- if $enabled -}}
{{- true -}}
{{- end -}}
{{- end -}}

{{/*
Escape JAAS value for quoted string contexts.
*/}}
{{- define "fluss.sasl.escapeJaasValue" -}}
{{- . | toString | replace "\\" "\\\\" | replace "\"" "\\\"" -}}
{{- end -}}

{{/*
Return true if ZooKeeper SASL is enabled
*/}}
{{- define "fluss.zookeeper.sasl.enabled" -}}
{{- $zkSaslUsername := default "" .Values.security.zookeeperSasl.username | trim -}}
{{- $zkSaslPassword := default "" .Values.security.zookeeperSasl.password | trim -}}
{{- if or (ne $zkSaslUsername "") (ne $zkSaslPassword "") -}}
{{- true -}}
{{- end -}}
{{- end -}}

{{/*
Return true if any JAAS configuration is required
*/}}
{{- define "fluss.jaas.required" -}}
{{- if or (include "fluss.sasl.enabled" .) (include "fluss.zookeeper.sasl.enabled" .) -}}
{{- true -}}
{{- end -}}
{{- end -}}

{{/*
Return true if listener protocol is SASL
Usage: include "fluss.listener.sasl.enabled" (dict "root" . "listener" "internal")
*/}}
{{- define "fluss.listener.sasl.enabled" -}}
{{- $listener := index .root.Values.listeners .listener -}}
{{- if and $listener $listener.protocol (regexFind "SASL" (upper $listener.protocol)) -}}
{{- true -}}
{{- end -}}
{{- end -}}

{{/*
Return upper-cased SASL mechanism for a listener (defaults to PLAIN)
Usage: include "fluss.listener.sasl.mechanism" (dict "root" . "listener" "internal")
*/}}
{{- define "fluss.listener.sasl.mechanism" -}}
{{- $listener := index .root.Values.listeners .listener -}}
{{- $security := default (dict) $listener.security -}}
{{- $mechanism := default "PLAIN" $security.mechanism -}}
{{- upper $mechanism -}}
{{- end -}}

{{/*
Validate SASL mechanism and users for a listener.
Fails if mechanism is not PLAIN or if any user has empty username/password.
Usage: include "fluss.sasl.validateListener" (dict "listener" "internal" "mechanism" $mechanism "users" $users "saslEnabled" true)
*/}}
{{- define "fluss.sasl.validateListener" -}}
{{- if .saslEnabled -}}
{{- if ne .mechanism "PLAIN" -}}
{{- fail (printf "listeners.%s.security.mechanism must be PLAIN when listeners.%s.protocol is SASL, got %s" .listener .listener .mechanism) -}}
{{- end -}}
{{- range $idx, $user := .users -}}
{{- if or (empty $user.username) (empty $user.password) -}}
{{- fail (printf "listeners.%s.security.users[%d] must set both username and password" $.listener $idx) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Auto-generate internal SASL users when none are provided.
Returns a JSON-encoded list of user dicts. Preserves previously generated credentials from existing secret.
Usage: $internalUsers := include "fluss.sasl.autoGenerateInternalUsers" (dict "root" . "existingUsers" $users) | fromJsonArray
*/}}
{{- define "fluss.sasl.autoGenerateInternalUsers" -}}
{{- $users := .existingUsers -}}
{{- if gt (len $users) 0 -}}
{{- $users | toJson -}}
{{- else -}}
{{- $secretName := printf "%s-sasl-jaas-config" (include "fluss.fullname" .root) -}}
{{- $existingSecret := (lookup "v1" "Secret" .root.Release.Namespace $secretName) -}}
{{- $existingSecretData := default (dict) $existingSecret.data -}}
{{- $generatedUser := "" -}}
{{- $generatedPassword := "" -}}
{{- if hasKey $existingSecretData "internal-generated-username" -}}
{{- $generatedUser = (get $existingSecretData "internal-generated-username") | b64dec -}}
{{- end -}}
{{- if hasKey $existingSecretData "internal-generated-password" -}}
{{- $generatedPassword = (get $existingSecretData "internal-generated-password") | b64dec -}}
{{- end -}}
{{- if or (empty $generatedUser) (empty $generatedPassword) -}}
{{- $generatedUser = printf "internal-%s" ((randAlphaNum 12) | lower) -}}
{{- $generatedPassword = randAlphaNum 32 -}}
{{- end -}}
{{- list (dict "username" $generatedUser "password" $generatedPassword) | toJson -}}
{{- end -}}
{{- end -}}

{{/*
Render a JAAS server block for a named context with the given users.
Usage: include "fluss.sasl.jaasServerBlock" (dict "name" "internal" "users" $users)
*/}}
{{- define "fluss.sasl.jaasServerBlock" }}
    {{ .name }}.FlussServer {
       org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required
        {{- range .users }}
       "user_{{ include "fluss.sasl.escapeJaasValue" .username }}"="{{ include "fluss.sasl.escapeJaasValue" .password }}"
        {{- end }};
    };
{{- end -}}

{{/*
Render a JAAS client block for inter-node authentication.
Usage: include "fluss.sasl.jaasClientBlock" (dict "user" $user)
*/}}
{{- define "fluss.sasl.jaasClientBlock" }}
    FlussClient {
       org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required
       username="{{ include "fluss.sasl.escapeJaasValue" .user.username }}"
       password="{{ include "fluss.sasl.escapeJaasValue" .user.password }}";
    };
{{- end -}}

{{/*
Consolidate SASL configuration logic into a single context.
Returns a JSON object with:
  internalSaslEnabled: bool
  clientSaslEnabled: bool
  internalUsers: list of {username, password}
  clientUsers: list of {username, password}
  internalUsersAutogenerated: bool
Usage: $saslConfig := include "fluss.sasl.config" . | fromJson
*/}}
{{- define "fluss.sasl.config" -}}
{{- $internalSaslEnabled := eq (include "fluss.listener.sasl.enabled" (dict "root" . "listener" "internal")) "true" -}}
{{- $clientSaslEnabled := eq (include "fluss.listener.sasl.enabled" (dict "root" . "listener" "client")) "true" -}}
{{- $internalSecurity := default (dict) .Values.listeners.internal.security -}}
{{- $clientSecurity := default (dict) .Values.listeners.client.security -}}
{{- $internalUsersProvided := default (list) $internalSecurity.users -}}
{{- $internalUsersAutogenerated := and $internalSaslEnabled (eq (len $internalUsersProvided) 0) -}}
{{- $internalUsers := include "fluss.sasl.autoGenerateInternalUsers" (dict "root" . "existingUsers" $internalUsersProvided) | fromJsonArray -}}
{{- $clientUsers := default (list) $clientSecurity.users -}}
{{- $internalMechanism := include "fluss.listener.sasl.mechanism" (dict "root" . "listener" "internal") -}}
{{- $clientMechanism := include "fluss.listener.sasl.mechanism" (dict "root" . "listener" "client") -}}
{{- include "fluss.sasl.validateListener" (dict "listener" "internal" "mechanism" $internalMechanism "users" $internalUsers "saslEnabled" $internalSaslEnabled) -}}
{{- if and $clientSaslEnabled (eq (len $clientUsers) 0) -}}
{{- fail "listeners.client.security.users must contain at least one user when listeners.client.protocol is SASL" -}}
{{- end -}}
{{- include "fluss.sasl.validateListener" (dict "listener" "client" "mechanism" $clientMechanism "users" $clientUsers "saslEnabled" $clientSaslEnabled) -}}
{{- dict "internalSaslEnabled" $internalSaslEnabled "clientSaslEnabled" $clientSaslEnabled "internalUsers" $internalUsers "clientUsers" $clientUsers "internalUsersAutogenerated" $internalUsersAutogenerated | toJson -}}
{{- end -}}
