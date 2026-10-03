{{/*
Chart name, truncated to 63 characters.
*/}}
{{- define "grid-site.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Fully qualified app name. Uses fullnameOverride if set, otherwise combines
release name and chart name (deduplicating when the release name already
contains the chart name).
*/}}
{{- define "grid-site.fullname" -}}
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

{{/*
Chart label value: name-version.
*/}}
{{- define "grid-site.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Standard Kubernetes labels applied to every resource.
*/}}
{{- define "grid-site.labels" -}}
helm.sh/chart: {{ include "grid-site.chart" . }}
app.kubernetes.io/name: {{ include "grid-site.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Translate one deprecated value key to its current name. Setting both names is
ambiguous, so fail before rendering any CustomResources.
*/}}
{{- define "grid-site.rename-legacy-value" -}}
{{- $object := .object -}}
{{- $old := .old -}}
{{- $new := .new -}}
{{- if hasKey $object $old -}}
  {{- if hasKey $object $new -}}
    {{- fail (printf "grid-site: remove deprecated value %q; set only %q" $old $new) -}}
  {{- end -}}
  {{- $_ := set $object $new (get $object $old) -}}
  {{- $_ := unset $object $old -}}
{{- end -}}
{{- end -}}

{{/*
Translate deprecated InferenceProvider value keys before the template emits
the CustomResource.
*/}}
{{- define "grid-site.normalize-inference-provider" -}}
{{- $provider := . -}}
{{- include "grid-site.rename-legacy-value" (dict "object" $provider "old" "routingClusterRef" "new" "clusterName") -}}
{{- include "grid-site.rename-legacy-value" (dict "object" $provider "old" "gatewayRef" "new" "providerGateway") -}}
{{- $metrics := get $provider "metricsConfig" -}}
{{- if kindIs "map" $metrics -}}
  {{- include "grid-site.rename-legacy-value" (dict "object" $metrics "old" "metricsEndpoint" "new" "endpoint") -}}
{{- end -}}
{{- $auth := get $provider "auth" -}}
{{- if kindIs "map" $auth -}}
  {{- include "grid-site.rename-legacy-value" (dict "object" $auth "old" "manual" "new" "credentialsManagedExternally") -}}
{{- end -}}
{{- end -}}

{{/*
Normalize values once per render, in place and idempotently. Listing peers selects the
enrolled multi-cluster defaults: site discovery on, and the TLS Secrets the operator
writes. inferenceProviders keyed by name become the list the template reads.
*/}}
{{- define "grid-site.normalize" -}}
{{- $v := .Values }}
{{- $net := $v.gridNetwork }}
{{- if kindIs "map" $net }}
  {{- include "grid-site.rename-legacy-value" (dict "object" $net "old" "gatewayRefs" "new" "consumerGateways") -}}
  {{- $gateways := get $net "consumerGateways" -}}
  {{- if kindIs "slice" $gateways -}}
    {{- range $gateway := $gateways -}}
      {{- if kindIs "map" $gateway -}}
        {{- include "grid-site.rename-legacy-value" (dict "object" $gateway "old" "localSiteName" "new" "siteName") -}}
        {{- include "grid-site.rename-legacy-value" (dict "object" $gateway "old" "consumerConfig" "new" "praxisConfig") -}}
        {{- $praxis := get $gateway "praxisConfig" -}}
        {{- if kindIs "map" $praxis -}}
          {{- include "grid-site.rename-legacy-value" (dict "object" $praxis "old" "enabled" "new" "generate") -}}
          {{- include "grid-site.rename-legacy-value" (dict "object" $praxis "old" "credentialMountBase" "new" "credentialMountPath") -}}
        {{- end -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}
  {{- $tls := get $net "tls" -}}
  {{- if kindIs "map" $tls -}}
    {{- include "grid-site.rename-legacy-value" (dict "object" $tls "old" "swimKeyRef" "new" "swimKeySecretRef") -}}
  {{- end -}}
{{- end -}}
{{- $site := get $v "gridSite" -}}
{{- if kindIs "map" $site -}}
  {{- include "grid-site.rename-legacy-value" (dict "object" $site "old" "egress" "new" "gatewayEndpoint") -}}
{{- end -}}
{{- if $v.peers }}
{{- if kindIs "invalid" $net.autoDiscoverSites }}{{- $_ := set $net "autoDiscoverSites" true }}{{- end }}
{{- if not $net.tls }}
{{- $ns := .Release.Namespace }}
{{- $_ := set $net "tls" (dict
  "siteSecretRef" (dict "name" "grid-site-identity" "namespace" $ns)
  "caSecretRef" (dict "name" "grid-ca" "namespace" $ns)
  "swimKeySecretRef" (dict "name" "grid-swim-key" "namespace" $ns)) }}
{{- end }}
{{- end }}
{{- if kindIs "map" $v.inferenceProviders }}
{{- $list := list }}
{{- range $name := keys $v.inferenceProviders | sortAlpha }}
{{- $p := deepCopy (get $v.inferenceProviders $name | default dict) }}
{{- include "grid-site.normalize-inference-provider" $p }}
{{- $model := $p.model | default $name }}
{{- $_ := unset $p "model" }}
{{- $list = append $list (merge $p (dict
  "name" $name
  "gridNetworkRef" $net.name
  "providerKind" "vllm"
  "backendKind" "local_model"
  "models" (list (dict "name" $model "capabilities" (list "text_generation"))))) }}
{{- end }}
{{- $_ := set $v "inferenceProviders" $list }}
{{- else if kindIs "slice" $v.inferenceProviders }}
{{- $list := list }}
{{- range $provider := $v.inferenceProviders }}
{{- $p := deepCopy $provider }}
{{- if kindIs "map" $p }}
  {{- include "grid-site.normalize-inference-provider" $p }}
{{- end }}
{{- $list = append $list $p }}
{{- end }}
{{- $_ := set $v "inferenceProviders" $list }}
{{- end }}
{{- end }}
