{{/*
Chart name, truncated to 63 characters.
*/}}
{{- define "praxis-gateway.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Fully qualified app name.
*/}}
{{- define "praxis-gateway.fullname" -}}
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
{{- define "praxis-gateway.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Standard Kubernetes labels applied to every resource.
*/}}
{{- define "praxis-gateway.labels" -}}
helm.sh/chart: {{ include "praxis-gateway.chart" . }}
{{ include "praxis-gateway.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Selector labels used by Deployment matchLabels and Service selectors.
*/}}
{{- define "praxis-gateway.selectorLabels" -}}
app.kubernetes.io/name: {{ include "praxis-gateway.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Container image reference.
Defaults the tag to the chart appVersion when no digest or tag is set.
*/}}
{{- define "praxis-gateway.image" -}}
{{- if .Values.image.digest }}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- else }}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) }}
{{- end }}
{{- end }}

{{/*
Overlay-sync image reference.
Defaults the tag to the chart appVersion when empty.
*/}}
{{- define "praxis-gateway.overlaySyncImage" -}}
{{- printf "%s:%s" .Values.overlay.sidecar.image.repository (default .Chart.AppVersion .Values.overlay.sidecar.image.tag) }}
{{- end }}

{{/*
Validate image digest format when provided.
*/}}
{{- define "praxis-gateway.validateDigest" -}}
{{- if and .Values.image.digest (not (regexMatch "^sha256:[0-9a-f]{64}$" .Values.image.digest)) }}
{{- fail "image.digest must be in the form sha256:<64 hex characters>" }}
{{- end }}
{{- end }}

{{/*
Validate required config ConfigMap name.
*/}}
{{- define "praxis-gateway.validateConfig" -}}
{{- if .Values.gatewayConfig.render }}
{{- if not (trim (toString .Values.gatewayConfig.model)) }}
{{- fail "gatewayConfig.model is required when gatewayConfig.render is true, and cannot be blank" }}
{{- end }}
{{- if not .Values.gatewayConfig.backends }}
{{- fail "gatewayConfig.backends needs at least one backend when gatewayConfig.render is true" }}
{{- end }}
{{- $auth := .Values.gatewayConfig.auth }}
{{- if not $auth.mode }}
{{- fail "gatewayConfig.auth.mode is required when gatewayConfig.render is true: api-key (needs an image with praxis-policy 0.4 or later) or none (only behind an authenticating front)" }}
{{- end }}
{{- if and (eq $auth.mode "none") .Values.service.enabled (has .Values.service.type (list "LoadBalancer" "NodePort")) (not $auth.allowUnauthenticatedExposure) }}
{{- fail (printf "gatewayConfig.auth.mode none with a %s Service exposes unauthenticated inference; use api-key, a ClusterIP Service behind an authenticating front, or set gatewayConfig.auth.allowUnauthenticatedExposure" .Values.service.type) }}
{{- end }}
{{- if and $auth.validateCA.configMap $auth.validateCA.secret }}
{{- fail "gatewayConfig.auth.validateCA: set configMap or secret, not both" }}
{{- end }}
{{- if eq $auth.mode "api-key" }}
{{- if not .Values.gatewayConfig.auth.validateUrl }}
{{- fail "gatewayConfig.auth.validateUrl is required when gatewayConfig.auth.mode is api-key" }}
{{- end }}
{{- if not (hasPrefix "https://" .Values.gatewayConfig.auth.validateUrl) }}
{{- fail "gatewayConfig.auth.validateUrl must be https: a plaintext validate call ships the credential in the clear" }}
{{- end }}
{{- if regexMatch "^https://(\\[|[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+([:/]|$))" .Values.gatewayConfig.auth.validateUrl }}
{{- fail "gatewayConfig.auth.validateUrl must name a host, not an IP address: https to an IP literal has no SNI to verify" }}
{{- end }}
{{- /* The chart-default image predates praxis-policy 0.4, which adds identity/api-key. */}}
{{- if eq (include "praxis-gateway.image" .) "ghcr.io/praxis-proxy/ai:0.4.0" }}
{{- fail "gatewayConfig.auth.mode api-key is unsupported on the default image ghcr.io/praxis-proxy/ai:0.4.0: its policy engine lacks identity/api-key (praxis-policy 0.4 or later). Set image to a build that registers it, or use auth.mode none behind an authenticating front." }}
{{- end }}
{{- end }}
{{- include "praxis-gateway.validateBackends" . }}
{{- else if not .Values.config.existingConfigMap }}
{{- fail "config.existingConfigMap is required (or set gatewayConfig.render: true)" }}
{{- end }}
{{- end }}

{{/*
Validate each backend's effective transport. mutual_tls presents the gateway's
grid identity (the tls mount) and needs a sni naming the peer; plaintext must not
carry a sni.
*/}}
{{- define "praxis-gateway.validateBackends" -}}
{{- $tlsEnabled := .Values.tls.enabled }}
{{- $seen := dict }}
{{- range .Values.gatewayConfig.backends }}
{{- if hasKey $seen .cluster }}
{{- fail (printf "gatewayConfig.backends: cluster %q is listed twice; cluster names must be unique" .cluster) }}
{{- end }}
{{- $_ := set $seen .cluster true }}
{{- $mode := (.transport).mode | default (ternary "mutual_tls" "plaintext" $tlsEnabled) }}
{{- if eq $mode "mutual_tls" }}
{{- if not $tlsEnabled }}
{{- fail (printf "backend %q uses mutual_tls but tls.enabled is false: no grid identity is mounted to present" .cluster) }}
{{- end }}
{{- if not (.transport).sni }}
{{- fail (printf "backend %q uses mutual_tls but sets no transport.sni to verify the peer against" .cluster) }}
{{- end }}
{{- else if eq $mode "plaintext" }}
{{- if (.transport).sni }}
{{- fail (printf "backend %q is plaintext but sets transport.sni; sni belongs to a TLS transport" .cluster) }}
{{- end }}
{{- else if eq $mode "tls" }}
{{- if regexMatch "^(\\[|[0-9.]+$)" (include "praxis-gateway.backendSni" .) }}
{{- fail (printf "backend %q uses tls to an IP endpoint without transport.sni: set transport.sni to a DNS name on the certificate, or use the Service hostname as the endpoint" .cluster) }}
{{- end }}
{{- else }}
{{- fail (printf "backend %q transport.mode must be mutual_tls, tls, or plaintext, got %q" .cluster $mode) }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Validate enabled mounts have a non-empty resource name.
*/}}
{{- define "praxis-gateway.validateMounts" -}}
{{- if and .Values.overlay.enabled (not .Values.overlay.existingConfigMap) }}
{{- fail "overlay.existingConfigMap is required when overlay.enabled is true" }}
{{- end }}
{{- if and .Values.overlay.enabled .Values.overlay.sidecar.enabled (not .Values.overlay.sidecar.expectedNetwork) }}
{{- fail "overlay.sidecar.expectedNetwork is required when overlay sidecar is enabled" }}
{{- end }}
{{- if and .Values.overlay.enabled .Values.overlay.sidecar.enabled (not .Values.overlay.sidecar.expectedLocalSite) }}
{{- fail "overlay.sidecar.expectedLocalSite is required when overlay sidecar is enabled" }}
{{- end }}
{{- if and .Values.tls.enabled (not .Values.tls.existingSecret) }}
{{- fail "tls.existingSecret is required when tls.enabled is true" }}
{{- end }}
{{- if and .Values.gatewayConfig.listenerTls.enabled (not .Values.gatewayConfig.listenerTls.existingSecret) }}
{{- fail "gatewayConfig.listenerTls.existingSecret is required when gatewayConfig.listenerTls.enabled is true" }}
{{- end }}
{{- end }}

{{/*
Listener port name: port.name when set, else https when the listener terminates TLS.
*/}}
{{- define "praxis-gateway.portName" -}}
{{- .Values.port.name | default (ternary "https" "http" .Values.gatewayConfig.listenerTls.enabled) -}}
{{- end }}

{{/*
Probe with an empty tcpSocket pointed at the listener port.
*/}}
{{- define "praxis-gateway.probe" -}}
{{- $probe := deepCopy (index . 0) -}}
{{- $root := index . 1 -}}
{{- if or $probe.httpGet $probe.exec $probe.grpc -}}
{{- $_ := unset $probe "tcpSocket" -}}
{{- else if and (hasKey $probe "tcpSocket") (not (($probe.tcpSocket | default dict).port)) -}}
{{- $_ := set $probe "tcpSocket" (dict "port" (include "praxis-gateway.portName" $root)) -}}
{{- end -}}
{{- toYaml $probe -}}
{{- end }}

{{/*
Whether a label selector matches everything: absent, {}, or empty matchLabels and
matchExpressions. Emits "true" or nothing.
*/}}
{{- define "praxis-gateway.selectsAll" -}}
{{- $sel := . | default dict -}}
{{- if and (not $sel.matchLabels) (not $sel.matchExpressions) -}}
true
{{- end -}}
{{- end }}

{{/*
SNI for a tls backend: transport.sni, else the first endpoint's host.
*/}}
{{- define "praxis-gateway.backendSni" -}}
{{- if (.transport).sni -}}
{{- .transport.sni -}}
{{- else -}}
{{- regexReplaceAll ":[0-9]+$" (first .endpoints) "" -}}
{{- end -}}
{{- end }}
