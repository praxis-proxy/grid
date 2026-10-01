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
Validate the praxis.yaml source and the values it needs.
praxisConfig.source picks who writes praxis.yaml:
  byo:      the user creates the ConfigMap named in praxisConfig.configMapName.
  operator: the Grid operator creates it, from a GridNetwork gatewayRef with
            consumerConfig.enabled. praxisConfig.configMapName must match its configMapName,
            and defaults to the operator's default name.
  render:   this chart creates it from praxisConfig.render.
*/}}
{{- define "praxis-gateway.validateConfig" -}}
{{- if not (has .Values.praxisConfig.source (list "byo" "operator" "render")) }}
{{- fail (printf "praxisConfig.source %q is not supported. Use byo, operator, or render." (toString .Values.praxisConfig.source)) }}
{{- end }}
{{- if eq .Values.praxisConfig.source "render" }}
{{- if not (trim (toString .Values.praxisConfig.render.model)) }}
{{- fail "praxisConfig.render.model is required when praxisConfig.source is render, and cannot be blank" }}
{{- end }}
{{- if not .Values.praxisConfig.render.backends }}
{{- fail "praxisConfig.render.backends needs at least one backend when praxisConfig.source is render" }}
{{- end }}
{{- $auth := .Values.praxisConfig.render.auth }}
{{- if not $auth.mode }}
{{- fail "praxisConfig.render.auth.mode is required when praxisConfig.source is render: api-key (needs an image with praxis-policy 0.4 or later) or none (only behind an authenticating front)" }}
{{- end }}
{{- if and (eq $auth.mode "none") .Values.service.enabled (has .Values.service.type (list "LoadBalancer" "NodePort")) (not $auth.allowUnauthenticatedExposure) }}
{{- fail (printf "praxisConfig.render.auth.mode none with a %s Service exposes unauthenticated inference; use api-key, a ClusterIP Service behind an authenticating front, or set praxisConfig.render.auth.allowUnauthenticatedExposure" .Values.service.type) }}
{{- end }}
{{- if and $auth.validateCA.configMap $auth.validateCA.secret }}
{{- fail "praxisConfig.render.auth.validateCA: set configMap or secret, not both" }}
{{- end }}
{{- if eq $auth.mode "api-key" }}
{{- if not .Values.praxisConfig.render.auth.validateUrl }}
{{- fail "praxisConfig.render.auth.validateUrl is required when praxisConfig.render.auth.mode is api-key" }}
{{- end }}
{{- if not (hasPrefix "https://" .Values.praxisConfig.render.auth.validateUrl) }}
{{- fail "praxisConfig.render.auth.validateUrl must be https: a plaintext validate call ships the credential in the clear" }}
{{- end }}
{{- if regexMatch "^https://(\\[|[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+([:/]|$))" .Values.praxisConfig.render.auth.validateUrl }}
{{- fail "praxisConfig.render.auth.validateUrl must name a host, not an IP address: https to an IP literal has no SNI to verify" }}
{{- end }}
{{- /* The chart-default image predates praxis-policy 0.4, which adds identity/api-key. */}}
{{- if eq (include "praxis-gateway.image" .) "ghcr.io/praxis-proxy/ai:0.4.0" }}
{{- fail "praxisConfig.render.auth.mode api-key is unsupported on the default image ghcr.io/praxis-proxy/ai:0.4.0: its policy engine lacks identity/api-key (praxis-policy 0.4 or later). Set image to a build that registers it, or use auth.mode none behind an authenticating front." }}
{{- end }}
{{- end }}
{{- include "praxis-gateway.validateBackends" . }}
{{- else if and (eq .Values.praxisConfig.source "byo") (not .Values.praxisConfig.configMapName) }}
{{- fail "praxisConfig.configMapName is required when praxisConfig.source is byo. Set it to the ConfigMap that holds praxis.yaml, or use praxisConfig.source: operator or render." }}
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
{{- range .Values.praxisConfig.render.backends }}
{{- if hasKey $seen .cluster }}
{{- fail (printf "praxisConfig.render.backends: cluster %q is listed twice; cluster names must be unique" .cluster) }}
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
{{- /*
The operator's praxis.yaml has no listener TLS and no upstream_ca_file, so these
mounts would do nothing. Fail instead of serving plaintext on an https port.
*/}}
{{- if eq .Values.praxisConfig.source "operator" }}
{{- if .Values.listenerTls.secretName }}
{{- fail "listenerTls.secretName is not supported with praxisConfig.source operator: the Grid operator's praxis.yaml has no listener TLS. Terminate TLS in front of the gateway, or use source byo or render." }}
{{- end }}
{{- if .Values.upstreamCA.secretName }}
{{- fail "upstreamCA.secretName is not supported with praxisConfig.source operator: the Grid operator's praxis.yaml has no upstream_ca_file. Use source byo or render." }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Listener port name: port.name when set, else https when the listener terminates TLS.
*/}}
{{- define "praxis-gateway.portName" -}}
{{- .Values.port.name | default (ternary "https" "http" (not (empty .Values.listenerTls.secretName))) -}}
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

{{/*
Name of the praxis.yaml ConfigMap the pod mounts.
render: the ConfigMap this chart creates, <fullname>-config.
operator: defaults to praxis-consumer-config, the Grid operator's default
consumerConfig.configMapName.
byo: praxisConfig.configMapName.
*/}}
{{- define "praxis-gateway.configMapName" -}}
{{- if eq .Values.praxisConfig.source "render" }}
{{- printf "%s-config" (include "praxis-gateway.fullname" .) }}
{{- else if eq .Values.praxisConfig.source "operator" }}
{{- .Values.praxisConfig.configMapName | default "praxis-consumer-config" }}
{{- else }}
{{- .Values.praxisConfig.configMapName }}
{{- end }}
{{- end }}
