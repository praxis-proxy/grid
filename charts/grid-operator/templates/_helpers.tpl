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
{{- /* The signals Service renders its own policy, so judge the one it renders. */}}
{{- if and (.Values.signals).enabled .Values.signals.service.enabled (eq (.Values.signals.service.type | default .Values.swim.service.type) "LoadBalancer") (eq (toString .Values.signals.service.externalTrafficPolicy) "Cluster") }}
{{- fail "signals on a LoadBalancer need signals.service.externalTrafficPolicy Local: the listener caps handshakes per source address" }}
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
Translate deprecated InferenceProvider value keys before the template emits
the CustomResource. Setting both names is ambiguous, so fail before rendering.
*/}}
{{- define "grid-operator.rename-legacy-value" -}}
{{- $object := .object -}}
{{- $old := .old -}}
{{- $new := .new -}}
{{- if hasKey $object $old -}}
  {{- if hasKey $object $new -}}
    {{- if empty (get $object $old) -}}
      {{- $_ := unset $object $old -}}
    {{- else -}}
      {{- fail (printf "grid-operator: remove deprecated value %q; set only %q" $old $new) -}}
    {{- end -}}
  {{- else -}}
    {{- $_ := set $object $new (get $object $old) -}}
    {{- $_ := unset $object $old -}}
  {{- end -}}
{{- end -}}
{{- end }}

{{/* Normalize deprecated provider values before rendering current CRD fields. */}}
{{- define "grid-operator.normalize-inference-provider" -}}
{{- $provider := . -}}
{{- include "grid-operator.rename-legacy-value" (dict "object" $provider "old" "routingClusterRef" "new" "clusterName") -}}
{{- include "grid-operator.rename-legacy-value" (dict "object" $provider "old" "gatewayRef" "new" "providerGateway") -}}
{{- $metrics := get $provider "metricsConfig" -}}
{{- if kindIs "map" $metrics -}}
  {{- include "grid-operator.rename-legacy-value" (dict "object" $metrics "old" "metricsEndpoint" "new" "endpoint") -}}
{{- end -}}
{{- $auth := get $provider "auth" -}}
{{- if kindIs "map" $auth -}}
  {{- include "grid-operator.rename-legacy-value" (dict "object" $auth "old" "manual" "new" "credentialsManagedExternally") -}}
{{- end -}}
{{- end }}

{{/*
Normalize values once per render, in place and idempotently. site.name and grid.seeds
default swim.siteName and swim.seeds, and a grid.id turns signals on for grid.signals poll and
defaults a LoadBalancer SWIM Service and the grid-gateway Service the GridNetwork names. Enrollment, the
cross-cluster path, defaults the site name to swim.siteName (and back), the URL to the
in-cluster grid-enrollment Service, a LoadBalancer SWIM Service, and the grid-gateway
Service. Without enrollment nothing changes.
*/}}
{{- define "grid-operator.normalize" -}}
{{- $v := .Values }}
{{- $e := $v.enrollment | default dict }}
{{- $svc := $v.swim.service }}
{{- $grid := $v.grid | default dict }}
{{- with ($v.site | default dict).name }}{{- if not $v.swim.siteName }}{{- $_ := set $v.swim "siteName" . }}{{- end }}{{- end }}
{{- with $grid.seeds }}{{- $_ := set $grid "seeds" (include "grid-operator.seedList" (dict "raw" . "port" ($svc.port | default 7946) "key" "grid.seeds") | fromYamlArray) }}{{- end }}
{{- with $grid.seeds }}{{- if not $v.swim.seeds }}{{- $_ := set $v.swim "seeds" (join "," .) }}{{- end }}{{- end }}
{{- if $grid.id }}
{{- if eq ($grid.signals | default "") "poll" }}{{- $_ := set $v.signals "enabled" true }}{{- end }}
{{- if kindIs "invalid" $svc.enabled }}
{{- $_ := set $svc "enabled" true }}
{{- if not $svc.type }}{{- $_ := set $svc "type" "LoadBalancer" }}{{- end }}
{{- end }}
{{- if not $v.gateway.serviceName }}{{- $_ := set $v.gateway "serviceName" "grid-gateway" }}{{- end }}
{{- end }}
{{- /* A hub names its in-cluster signer whether or not it enrolls through it: a hub whose
     identity bootstrap issued still renews there. */}}
{{- if or $e.enabled $e.hub }}
{{- if not $v.rbac.enrollmentNamespace }}{{- $_ := set $v.rbac "enrollmentNamespace" "grid-enrollment" }}{{- end }}
{{- end }}
{{- if and $e.hub (not $e.url) }}
{{- $_ := set $e "url" (printf "https://%s.%s.svc:%v" ($e.hubService | default "grid-enrollment") $v.rbac.enrollmentNamespace ($e.hubPort | default 8443)) }}
{{- end }}
{{- if $e.enabled }}
{{- if not $e.siteName }}{{- $_ := set $e "siteName" $v.swim.siteName }}{{- end }}
{{- if not $v.swim.siteName }}{{- $_ := set $v.swim "siteName" $e.siteName }}{{- end }}
{{- if not $e.url }}{{- fail "enrollment.url is required: name the hub's signer, or set enrollment.hub=true where grid-enrollment runs on this cluster" }}{{- end }}
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
{{- $cloud := .root.Values.platform | default "" -}}
{{- $internal := eq (.root.Values.loadBalancerScope | default "internal") "internal" -}}
{{- /* Each cloud reads only its own keys, so "any" carries all of them. */ -}}
{{- if has $cloud (list "aws" "any") -}}
{{- $_ := set $platform "service.beta.kubernetes.io/aws-load-balancer-type" "nlb" -}}
{{- /* Explicit false: the AWS Load Balancer Controller treats an absent annotation as internal. */ -}}
{{- $_ := set $platform "service.beta.kubernetes.io/aws-load-balancer-internal" (toString $internal) -}}
{{- /* A zone with no target drops traffic: SWIM advertises one zonal address, and signals moves zones on rollout. */ -}}
{{- $_ := set $platform "service.beta.kubernetes.io/aws-load-balancer-cross-zone-load-balancing-enabled" "true" -}}
{{- end -}}
{{- if and $internal (has $cloud (list "azure" "any")) -}}
{{- $_ := set $platform "service.beta.kubernetes.io/azure-load-balancer-internal" "true" -}}
{{- end -}}
{{- if and $internal (has $cloud (list "gcp" "any")) -}}
{{- $_ := set $platform "networking.gke.io/load-balancer-type" "Internal" -}}
{{- /* Sites sit in different regions, and a GCP internal load balancer serves only its own without this. */ -}}
{{- $_ := set $platform "networking.gke.io/internal-load-balancer-allow-global-access" "true" -}}
{{- end -}}
{{- $merged := merge (deepCopy $own) $platform -}}
{{- if $merged }}
{{- toYaml $merged }}
{{- end }}
{{- end -}}

{{/* A value separated by spaces or commas, as a list, so one `--set` needs no braces. */}}
{{- define "grid-operator.entryList" -}}
{{- $raw := . | default "" | replace "," " " -}}
{{- $out := list -}}
{{- range (splitList " " $raw) -}}
{{- $p := trim . -}}
{{- if $p -}}
{{- $out = append $out $p -}}
{{- end -}}
{{- end -}}
{{- toYaml $out -}}
{{- end -}}

{{/* One SWIM endpoint from `entry`, read by colon count as the operator reads it: [ipv6]:port
     and host:port are taken as given, a bare IPv6 address is bracketed, and the rest get `port`. */}}
{{- define "grid-operator.swimEndpoint" -}}
{{- $parts := len (splitList ":" .entry) -}}
{{- if and (hasPrefix "[" .entry) (hasSuffix "]" .entry) -}}
{{- printf "%s:%v" .entry .port -}}
{{- else if and (or (hasPrefix "[" .entry) (eq $parts 2)) (or (hasPrefix ":" .entry) (hasSuffix ":" .entry)) -}}
{{- fail (printf "%s is not host:port" .entry) -}}
{{- else if or (hasPrefix "[" .entry) (eq $parts 2) -}}
{{- .entry -}}
{{- else if gt $parts 2 -}}
{{- printf "[%s]:%v" .entry .port -}}
{{- else -}}
{{- printf "%s:%v" .entry .port -}}
{{- end -}}
{{- end -}}

{{/* `raw`, a list or a string in the `peers` shape, as a list of SWIM endpoints at `port`.
     Given `key`, the value's name, a value naming no endpoint refuses to render instead of
     being dropped without a word. */}}
{{- define "grid-operator.seedList" -}}
{{- $raw := .raw -}}
{{- if kindIs "slice" $raw -}}{{- $raw = join " " $raw -}}{{- end -}}
{{- $out := list -}}
{{- range (include "grid-operator.entryList" $raw | fromYamlArray) -}}
{{- $out = append $out (include "grid-operator.swimEndpoint" (dict "entry" . "port" $.port)) -}}
{{- end -}}
{{- if and .key (not $out) -}}
{{- fail (printf "%s names no endpoint: list at least one, or leave it unset" .key) -}}
{{- end -}}
{{- toYaml $out -}}
{{- end -}}

{{/* `swim.seeds` when set, else `peers`, as comma-joined SWIM endpoints at the SWIM port. */}}
{{- define "grid-operator.swimSeeds" -}}
{{- $port := (.Values.swim.service).port | default 7946 -}}
{{- $args := dict "raw" .Values.peers "port" $port -}}
{{- with .Values.swim.seeds -}}{{- $args = dict "raw" . "port" $port "key" "swim.seeds" -}}{{- end -}}
{{- include "grid-operator.seedList" $args | fromYamlArray | join "," -}}
{{- end -}}

{{/* `own` when set, else each peer as a host route. A name yields none: no CIDR to derive. */}}
{{- define "grid-operator.peerSourceRanges" -}}
{{- $own := .own | default list -}}
{{- if $own -}}
{{- toYaml $own -}}
{{- else -}}
{{- $ranges := list -}}
{{- range (include "grid-operator.entryList" .root.Values.peers | fromYamlArray) -}}
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

{{/*
The PodDisruptionBudget's minAvailable and maxUnavailable as strings, empty when unset.
*/}}
{{- define "grid-operator.pdb.bounds" -}}
{{- $pdb := .Values.podDisruptionBudget | default dict }}
{{- $min := "" }}
{{- if not (kindIs "invalid" $pdb.minAvailable) }}{{ $min = toString $pdb.minAvailable }}{{ end }}
{{- $max := "" }}
{{- if not (kindIs "invalid" $pdb.maxUnavailable) }}{{ $max = toString $pdb.maxUnavailable }}{{ end }}
{{- dict "min" $min "max" $max | toJson }}
{{- end }}

{{/*
"true" when the PodDisruptionBudget renders: enabled true, or auto with more than one replica.
*/}}
{{- define "grid-operator.pdb.renders" -}}
{{- $enabled := toString ((.Values.podDisruptionBudget | default dict).enabled) }}
{{- if or (eq $enabled "true") (and (eq $enabled "auto") (gt (int .Values.replicaCount) 1)) }}true{{ end }}
{{- end }}

{{/*
Pods a bound of `value` covers out of `replicas`, rounding a percentage up as Kubernetes does.
*/}}
{{- define "grid-operator.pdb.pods" -}}
{{- if hasSuffix "%" .value }}
{{- div (add (mul (int (trimSuffix "%" .value)) .replicas) 99) 100 }}
{{- else }}
{{- int .value }}
{{- end }}
{{- end }}

{{/*
Refuse a PodDisruptionBudget that names both bounds, neither, or one no node drain can satisfy.
*/}}
{{- define "grid-operator.pdb.validate" -}}
{{- $enabled := toString ((.Values.podDisruptionBudget | default dict).enabled) }}
{{- if not (has $enabled (list "auto" "true" "false")) }}
{{- fail (printf "podDisruptionBudget.enabled must be auto, true or false, not %q" $enabled) }}
{{- end }}
{{- if include "grid-operator.pdb.renders" . }}
{{- $bounds := include "grid-operator.pdb.bounds" . | fromJson }}
{{- $replicas := int .Values.replicaCount }}
{{- if and $bounds.min $bounds.max }}
{{- fail "set one of podDisruptionBudget.minAvailable and podDisruptionBudget.maxUnavailable, not both" }}
{{- else if $bounds.min }}
{{- if ge (int (include "grid-operator.pdb.pods" (dict "value" $bounds.min "replicas" $replicas))) $replicas }}
{{- fail (printf "podDisruptionBudget.minAvailable %s with replicaCount %d would block node drains: no operator pod could ever be evicted" $bounds.min $replicas) }}
{{- end }}
{{- else if $bounds.max }}
{{- if le (int (include "grid-operator.pdb.pods" (dict "value" $bounds.max "replicas" $replicas))) 0 }}
{{- fail (printf "podDisruptionBudget.maxUnavailable %s would block node drains: no operator pod could ever be evicted" $bounds.max) }}
{{- end }}
{{- else }}
{{- fail "set podDisruptionBudget.minAvailable or podDisruptionBudget.maxUnavailable" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
A PodDisruptionBudget bound as YAML: a percentage quoted, a count as a number.
*/}}
{{- define "grid-operator.pdb.value" -}}
{{- if hasSuffix "%" . }}{{ quote . }}{{ else }}{{ int . }}{{ end }}
{{- end }}

{{/*
The preStop delay: as set, else none. Under HA the holder withdraws its own EndpointSlices, so
no selector Service waits on readiness for peers.
*/}}
{{- define "grid-operator.shutdownDelaySeconds" -}}
{{- if kindIs "invalid" .Values.shutdownDelaySeconds }}0{{ else }}{{ int .Values.shutdownDelaySeconds }}{{ end }}
{{- end }}

{{/*
The Services carrying signals that route to the lease holder alone.
*/}}
{{- define "grid-operator.holderSignalsServices" -}}
{{- if and (.Values.signals).enabled .Values.signals.service.enabled }}{{ include "grid-operator.fullname" . }}-signals{{ end }}
{{- end }}

{{/*
The Services carrying SWIM that route to the lease holder alone.
*/}}
{{- define "grid-operator.holderSwimServices" -}}
{{- if .Values.swim.service.enabled }}{{ include "grid-operator.fullname" . }}-swim{{ end }}
{{- end }}
