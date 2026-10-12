{{/*
Chart name, truncated to 63 characters.
*/}}
{{- define "grid-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Fully qualified app name. Uses fullnameOverride if set, otherwise combines
release name and chart name (deduplicating when the release name already
contains the chart name).
*/}}
{{- define "grid-operator.fullname" -}}
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
{{- define "grid-operator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Standard Kubernetes labels applied to every resource.
*/}}
{{- define "grid-operator.labels" -}}
helm.sh/chart: {{ include "grid-operator.chart" . }}
{{ include "grid-operator.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Selector labels used by Deployment matchLabels and Service selectors.
These must remain stable across upgrades.
*/}}
{{- define "grid-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "grid-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
ServiceAccount name. When create is true, defaults to the release fullname.
When create is false, defaults to "default" per Helm RBAC convention.
*/}}
{{- define "grid-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "grid-operator.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Operator container image reference. When digest is set, renders
repository@digest and ignores tag. Otherwise renders repository:tag
(tag defaults to Chart.AppVersion).
*/}}
{{- define "grid-operator.image" -}}
{{- if .Values.image.digest }}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- else }}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) }}
{{- end }}
{{- end }}

{{/*
Signals are reached through the SWIM Service.
*/}}
{{- define "grid-operator.validateSignals" -}}
{{- if and (.Values.signals).enabled (not .Values.swim.service.enabled) }}
{{- fail "signals.enabled needs swim.service.enabled: peers and the local gateway reach signals through the SWIM Service" }}
{{- end }}
{{- if and (.Values.signals).enabled (eq .Values.swim.service.type "LoadBalancer") (eq .Values.swim.service.externalTrafficPolicy "Cluster") }}
{{- fail "signals on a LoadBalancer need swim.service.externalTrafficPolicy Local: the listener caps handshakes per source address" }}
{{- end }}
{{- end }}

{{/*
Validate image digest format when provided.
*/}}
{{- define "grid-operator.validateDigest" -}}
{{- if and .Values.image.digest (not (regexMatch "^sha256:[0-9a-f]{64}$" .Values.image.digest)) }}
{{- fail "image.digest must be in the form sha256:<64 hex characters>" }}
{{- end }}
{{- end }}

{{/*
Flat-grid and enrollment defaults fill missing site identity, SWIM reachability,
and gateway references so the site can join the mesh and advertise its gateway.
In-place, idempotent normalization lets each template apply the same defaults
safely, regardless of render order. Explicit site names, seeds, and SWIM Service
settings take precedence; enrollment.siteName remains independent when supplied
alongside site.name or swim.siteName.
*/}}
{{- define "grid-operator.normalize" -}}
{{- $v := .Values }}
{{- $e := $v.enrollment | default dict }}
{{- $svc := $v.swim.service }}
{{- $grid := $v.grid | default dict }}
{{- with ($v.site | default dict).name }}{{- if not $v.swim.siteName }}{{- $_ := set $v.swim "siteName" . }}{{- end }}{{- end }}
{{- with $grid.seeds }}{{- if not $v.swim.seeds }}{{- $_ := set $v.swim "seeds" (join "," .) }}{{- end }}{{- end }}
{{- if $grid.id }}
{{- if eq ($grid.signals | default "") "poll" }}{{- $_ := set $v.signals "enabled" true }}{{- end }}
{{- if kindIs "invalid" $svc.enabled }}
{{- $_ := set $svc "enabled" true }}
{{- if not $svc.type }}{{- $_ := set $svc "type" "LoadBalancer" }}{{- end }}
{{- end }}
{{- if not $v.gateway.serviceName }}{{- $_ := set $v.gateway "serviceName" "grid-gateway" }}{{- end }}
{{- end }}
{{- if $e.enabled }}
{{- if not $e.siteName }}{{- $_ := set $e "siteName" $v.swim.siteName }}{{- end }}
{{- if not $v.swim.siteName }}{{- $_ := set $v.swim "siteName" $e.siteName }}{{- end }}
{{- if not $v.rbac.enrollmentNamespace }}{{- $_ := set $v.rbac "enrollmentNamespace" "grid-enrollment" }}{{- end }}
{{- if not $e.url }}{{- $_ := set $e "url" (printf "https://grid-enrollment.%s.svc:8443" $v.rbac.enrollmentNamespace) }}{{- end }}
{{- if not $v.gateway.serviceName }}{{- $_ := set $v.gateway "serviceName" "grid-gateway" }}{{- end }}
{{- if kindIs "invalid" $svc.enabled }}{{- $_ := set $svc "enabled" true }}{{- if not $svc.type }}{{- $_ := set $svc "type" "LoadBalancer" }}{{- end }}{{- end }}
{{- end }}
{{- if kindIs "invalid" $svc.enabled }}{{- $_ := set $svc "enabled" false }}{{- end }}
{{- if not $svc.type }}{{- $_ := set $svc "type" "ClusterIP" }}{{- end }}
{{- end }}

{{/*
Refuse a grid the chart cannot render.
*/}}
{{- define "grid-operator.validateGrid" -}}
{{- if and .Values.grid.id (not .Values.swim.siteName) }}
{{- fail "grid.id needs site.name: the GridSite and the SWIM identity are named after this site" }}
{{- end }}
{{- end }}

{{/*
Annotations that order a grid custom resource after its CRD under Argo CD.
*/}}
{{- define "grid-operator.afterCrds" -}}
argocd.argoproj.io/sync-wave: "1"
argocd.argoproj.io/sync-options: SkipDryRunOnMissingResource=true
{{- end }}

{{/*
RUST_LOG for the chart's Rust binaries: log.filter when set, else log.level.
*/}}
{{- define "grid-operator.rustLog" -}}
{{- $log := .Values.log | default dict -}}
{{- $log.filter | default $log.level | default "info" -}}
{{- end }}

{{/* Service annotations, the platform's merged under the caller's. AWS needs an NLB: the default carries no UDP. */}}
{{- define "grid-operator.serviceAnnotations" -}}
{{- $own := .own | default dict -}}
{{- $platform := dict -}}
{{- if eq (.root.Values.platform | default "") "aws" -}}
{{- $platform = dict "service.beta.kubernetes.io/aws-load-balancer-type" "nlb" -}}
{{- end -}}
{{- $merged := merge (deepCopy $own) $platform -}}
{{- if $merged }}
{{- toYaml $merged }}
{{- end }}
{{- end -}}

{{/* `peers` as a list. Spaces or commas, so one `--set` needs no braces. */}}
{{- define "grid-operator.peerList" -}}
{{- $raw := .Values.peers | default "" | replace "," " " -}}
{{- $out := list -}}
{{- range (splitList " " $raw) -}}
{{- $p := trim . -}}
{{- if $p -}}
{{- $out = append $out $p -}}
{{- end -}}
{{- end -}}
{{- toYaml $out -}}
{{- end -}}

{{/* `swim.seeds` when set, else each peer at the SWIM port. A peer with a port is taken as given. */}}
{{- define "grid-operator.swimSeeds" -}}
{{- if .Values.swim.seeds -}}
{{- .Values.swim.seeds -}}
{{- else -}}
{{- $port := (.Values.swim.service).port | default 7946 -}}
{{- $seeds := list -}}
{{- range (include "grid-operator.peerList" . | fromYamlArray) -}}
{{/* A bare IPv6 address has colons but no port, so bracket it; [addr]:port and host:port are taken as given. */}}
{{- if or (hasPrefix "[" .) (and (contains ":" .) (not (regexMatch "^[0-9a-fA-F:]+$" .))) -}}
{{- $seeds = append $seeds . -}}
{{- else if contains ":" . -}}
{{- $seeds = append $seeds (printf "[%s]:%v" . $port) -}}
{{- else -}}
{{- $seeds = append $seeds (printf "%s:%v" . $port) -}}
{{- end -}}
{{- end -}}
{{- join "," $seeds -}}
{{- end -}}
{{- end -}}

{{/* `own` when set, else each peer as a host route. A name yields none: no CIDR to derive. */}}
{{- define "grid-operator.peerSourceRanges" -}}
{{- $own := .own | default list -}}
{{- if $own -}}
{{- toYaml $own -}}
{{- else -}}
{{- $ranges := list -}}
{{- range (include "grid-operator.peerList" .root | fromYamlArray) -}}
{{- if regexMatch "^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$" . -}}
{{/* 300.0.0.1 matches the shape; an out-of-range octet would make a CIDR the API rejects,
     taking the Service with it, so refuse to render instead. */}}
{{- range $o := splitList "." . -}}
{{- if gt (int $o) 255 -}}
{{- fail (printf "peers: %q is not an IPv4 address" $o) -}}
{{- end -}}
{{- end -}}
{{- $ranges = append $ranges (printf "%s/32" .) -}}
{{- end -}}
{{- end -}}
{{- if $ranges -}}
{{- toYaml $ranges -}}
{{- end -}}
{{- end -}}
{{- end -}}

