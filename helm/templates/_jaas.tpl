{{/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/}}

{{/*
Renders the env entries for the JAAS init container: secretKeyRef for each
Secret-sourced credential, literal `value:` for each values-sourced one.
Usage:
  include "fluss.security.jaas.initContainer.env" .
*/}}
{{- define "fluss.security.jaas.initContainer.env" -}}
{{- $internalMechanism := include "fluss.security.listener.mechanism" (dict "context" .Values "listener" "internal") -}}
{{- $clientMechanism := include "fluss.security.listener.mechanism" (dict "context" .Values "listener" "client") -}}
{{- $zkEnabled := include "fluss.security.zookeeper.sasl.enabled" . -}}
{{- /* internal */ -}}
{{- if and (include "fluss.security.sasl.plain.enabled" .) (eq $internalMechanism "plain") }}
- name: {{ include "fluss.security.sasl.plain.internal.envVarName" "username" }}
{{- $ref := .Values.security.internal.sasl.plain.existingSecret | default (dict) }}
{{- if $ref.name }}
  valueFrom:
    secretKeyRef:
      name: {{ $ref.name }}
      key: {{ $ref.usernameKey | default "username" }}
{{- else }}
  value: {{ include "fluss.security.sasl.plain.internal.username" . | quote }}
{{- end }}
- name: {{ include "fluss.security.sasl.plain.internal.envVarName" "password" }}
{{- if $ref.name }}
  valueFrom:
    secretKeyRef:
      name: {{ $ref.name }}
      key: {{ $ref.passwordKey | default "password" }}
{{- else }}
  value: {{ include "fluss.security.sasl.plain.internal.password" . | quote }}
{{- end }}
{{- end }}
{{- /* client */ -}}
{{- if and (include "fluss.security.sasl.plain.enabled" .) (eq $clientMechanism "plain") }}
{{- range $idx, $user := .Values.security.client.sasl.plain.users | default (list) }}
{{- $ref := $user.existingSecret | default (dict) }}
- name: {{ include "fluss.security.sasl.plain.client.envVarName" (dict "field" "username" "idx" $idx) }}
{{- if $ref.name }}
  valueFrom:
    secretKeyRef:
      name: {{ $ref.name }}
      key: {{ $ref.usernameKey | default "username" }}
{{- else }}
  value: {{ $user.username | quote }}
{{- end }}
- name: {{ include "fluss.security.sasl.plain.client.envVarName" (dict "field" "password" "idx" $idx) }}
{{- if $ref.name }}
  valueFrom:
    secretKeyRef:
      name: {{ $ref.name }}
      key: {{ $ref.passwordKey | default "password" }}
{{- else }}
  value: {{ $user.password | quote }}
{{- end }}
{{- end }}
{{- end }}
{{- /* zookeeper */ -}}
{{- if $zkEnabled }}
- name: {{ include "fluss.security.sasl.plain.zookeeper.envVarName" "username" }}
{{- $zkRef := .Values.security.zookeeper.sasl.plain.existingSecret | default (dict) }}
{{- if $zkRef.name }}
  valueFrom:
    secretKeyRef:
      name: {{ $zkRef.name }}
      key: {{ $zkRef.usernameKey | default "username" }}
{{- else }}
  value: {{ .Values.security.zookeeper.sasl.plain.username | quote }}
{{- end }}
- name: {{ include "fluss.security.sasl.plain.zookeeper.envVarName" "password" }}
{{- if $zkRef.name }}
  valueFrom:
    secretKeyRef:
      name: {{ $zkRef.name }}
      key: {{ $zkRef.passwordKey | default "password" }}
{{- else }}
  value: {{ .Values.security.zookeeper.sasl.plain.password | quote }}
{{- end }}
{{- end }}
{{- end -}}

{{/*
Init container spec: mounts the rendered JAAS template ConfigMap at /tmpl,
resolves credential placeholders via envsubst, writes the result to the
shared emptyDir at /jaas, and chmods it 0400.
Usage:
  include "fluss.security.jaas.initContainer" .
*/}}
{{- define "fluss.security.jaas.initContainer" -}}
- name: render-jaas-config
  image: {{ include "fluss.image" . }}
  imagePullPolicy: {{ .Values.image.pullPolicy }}
  env:
    {{- include "fluss.security.jaas.initContainer.env" . | nindent 4 }}
  command:
    - /bin/sh
    - -ec
    - |
      umask 077
      envsubst < /tmpl/jaas.conf > /jaas/jaas.conf
      chmod 0400 /jaas/jaas.conf
      {{- if include "fluss.security.zookeeper.sasl.enabled" . }}
      cp /tmpl/zookeeper-client.properties /jaas/zookeeper-client.properties
      chmod 0400 /jaas/zookeeper-client.properties
      {{- end }}
  volumeMounts:
    - name: sasl-template
      mountPath: /tmpl
      readOnly: true
    - name: sasl-config
      mountPath: /jaas
{{- end -}}

{{/*
Pod-level volumes for the JAAS render path: the template ConfigMap (input)
and an in-memory emptyDir (output, shared with the main container).
Usage:
  include "fluss.security.jaas.volumes" .
*/}}
{{- define "fluss.security.jaas.volumes" -}}
- name: sasl-template
  configMap:
    name: {{ include "fluss.security.jaas.configName" . }}
- name: sasl-config
  emptyDir:
    medium: Memory
    sizeLimit: 1Mi
{{- end -}}
