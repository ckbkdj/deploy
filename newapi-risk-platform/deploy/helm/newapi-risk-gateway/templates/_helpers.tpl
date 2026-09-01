{{- define "newapi-risk-gateway.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "newapi-risk-gateway.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "newapi-risk-gateway.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{ include "newapi-risk-gateway.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}

{{- define "newapi-risk-gateway.selectorLabels" -}}
app.kubernetes.io/name: {{ include "newapi-risk-gateway.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "newapi-risk-gateway.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "newapi-risk-gateway.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "newapi-risk-gateway.secretName" -}}
{{- default (include "newapi-risk-gateway.fullname" .) .Values.secret.existingSecret }}
{{- end }}

{{- define "newapi-risk-gateway.kafkaTLSSecretName" -}}
{{- if .Values.kafkaTLS.existingSecret }}
{{- .Values.kafkaTLS.existingSecret }}
{{- else }}
{{- printf "%s-kafka-tls" (include "newapi-risk-gateway.fullname" .) }}
{{- end }}
{{- end }}
