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
Collects all warning messages from validation methods.
Usage:
  include "fluss.validateWarning" .
*/}}
{{- define "fluss.validateWarning" -}}
{{- $messages := list -}}

{{- $messages = append $messages (include "fluss.security.validateWarning" .) -}}

{{- $messages = without $messages "" -}}
{{- join "\n" $messages -}}
{{- end -}}

{{/*
Collects all error messages from validation methods.
Usage:
  include "fluss.validateError" .
*/}}
{{- define "fluss.validateError" -}}
{{- $messages := list -}}

{{- $messages = append $messages (include "fluss.security.validateError" .) -}}
{{- $messages = append $messages (include "fluss.metrics.validateError" .) -}}
{{- $messages = append $messages (include "fluss.secrets.validateError" .) -}}
{{- $messages = append $messages (include "fluss.names.validateError" .) -}}

{{- $messages = without $messages "" -}}
{{- join "\n" $messages -}}
{{- end -}}

{{/*
Global validation checks entry point.
Collects all warnings and errors, prints warnings and fails on errors.
Usage:
  include "fluss.validate" .
*/}}
{{- define "fluss.validate" -}}

{{- $warnMessages := list -}}
{{- $warnMessages = append $warnMessages (include "fluss.validateWarning" .) -}}
{{- $warnMessages = without $warnMessages "" -}}
{{- $warnMessage := join "\n" $warnMessages -}}

{{- $errMessages := list -}}
{{- $errMessages = append $errMessages (include "fluss.validateError" .) -}}
{{- $errMessages = without $errMessages "" -}}
{{- $errMessage := join "\n" $errMessages -}}

{{- if $warnMessage -}}
{{-   printf "\nVALUES WARNING:\n%s" $warnMessage -}}
{{- end -}}

{{- if $errMessage -}}
{{-   printf "\nVALUES VALIDATION:\n%s" $errMessage | fail -}}
{{- end -}}

{{- end -}}

{{/*
Validates that the generated resource names stay within the 63 character limit
Kubernetes imposes on DNS labels. Only applies with releaseScopedResourceNames,
where the longest generated name adds 30 characters to the release prefix.
Usage:
  include "fluss.names.validateError" .
*/}}
{{- define "fluss.names.validateError" -}}
{{- if .Values.releaseScopedResourceNames -}}
{{- $prefix := include "fluss.fullname" . -}}
{{- $longestSuffix := 30 -}}
{{- $maxPrefix := sub 63 $longestSuffix -}}
{{- if gt (len $prefix) (int $maxPrefix) -}}
{{- printf "resource name prefix %q is %d characters, but generated names must stay within 63 characters. Use a release name of at most %d characters, or set fullnameOverride." $prefix (len $prefix) (int $maxPrefix) -}}
{{- end -}}
{{- end -}}
{{- end -}}
