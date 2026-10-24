{{/* Enrollment values with defaults: the grid-ca-bundle Secret and grid-invite-<siteName>. */}}
{{- define "grid-operator.enrollment.settings" -}}
{{- $e := deepCopy (.Values.enrollment | default dict) }}
{{- $b := $e.caBundle | default dict }}
{{- if not (or $b.configMap $b.secret) }}
{{- $_ := set $e "caBundle" (merge (dict "secret" "grid-ca-bundle") $b) }}
{{- end }}
{{- $t := $e.tokenSecretRef | default dict }}
{{- if and (not $t.name) $e.siteName }}
{{- $_ := set $e "tokenSecretRef" (merge (dict "name" (printf "grid-invite-%s" $e.siteName)) $t) }}
{{- end }}
{{- toYaml $e }}
{{- end }}

{{- define "grid-operator.enrollment.validate" -}}
{{- $e := include "grid-operator.enrollment.settings" . | fromYaml }}
{{- if and $e.enabled (not .Values.grid.id) (or (ne ($e.identitySecretName | default "grid-site-identity") "grid-site-identity") (ne ($e.caSecretName | default "grid-ca") "grid-ca")) }}
{{- fail "enrollment.identitySecretName and caSecretName name the Secrets in the GridNetwork this chart renders: set grid.id, or keep grid-site-identity and grid-ca" }}
{{- end }}
{{- $raw := .Values.enrollment | default dict }}
{{- if and (not .Values.grid.id) (or $raw.url $raw.rotation (dig "caBundle" "configMap" "" $raw) (dig "caBundle" "secret" "" $raw) (dig "gridCaBundle" "configMap" "" $raw) (dig "gridCaBundle" "secret" "" $raw) (dig "tokenSecretRef" "name" "" $raw)) }}
{{- fail "enrollment.url, caBundle, gridCaBundle, tokenSecretRef and rotation render onto this chart's GridSite, which needs grid.id. Without it, set them on the GridSite the grid-site chart renders: gridSite.local" }}
{{- end }}
{{- if and $e.rotation (not (or $e.enabled $e.url)) }}
{{- fail "enrollment.rotation sits under signerEndpoint: set enrollment.url, or enrollment.enabled" }}
{{- end }}
{{- if and $e.url (not (hasPrefix "https://" $e.url)) }}
{{- fail "enrollment.url must be an https URL" }}
{{- end }}
{{- if and $e.enabled (not $e.siteName) }}
{{- fail "enrollment.siteName is required when enrollment.enabled is true" }}
{{- end }}
{{- if and $e.enabled .Values.grid.id }}
{{- if not (hasPrefix "https://" ($e.url | default "")) }}
{{- fail "enrollment.url must be an https URL when enrollment.enabled is true" }}
{{- end }}
{{- if not (dig "tokenSecretRef" "name" "" $e) }}
{{- fail "enrollment.tokenSecretRef.name is required when enrollment.enabled is true" }}
{{- end }}
{{- if not (or (dig "caBundle" "configMap" "" $e) (dig "caBundle" "secret" "" $e)) }}
{{- fail "enrollment.caBundle needs a configMap or a secret when enrollment.enabled is true" }}
{{- end }}
{{- end }}
{{- end }}

{{/* A BundleRef for a caBundle-shaped value. */}}
{{- define "grid-operator.enrollment.bundleRef" -}}
{{- if dig "configMap" "" . }}
kind: ConfigMap
name: {{ .configMap | quote }}
{{- else }}
kind: Secret
name: {{ dig "secret" "" . | quote }}
{{- end }}
key: {{ dig "key" "ca.crt" . | quote }}
{{- end }}

{{/* The local GridSite's spec.local: signerEndpoint with its rotation, and inviteSecretRef. A hub whose identity bootstrap issued sets only the URL, and renews pinned to the grid CA it holds. Unset, rotation renders Disabled unless grid.peerTrust is spiffe, since pin peer trust refuses a renewed leaf. */}}
{{- define "grid-operator.enrollment.siteSpec" -}}
{{- $e := include "grid-operator.enrollment.settings" . | fromYaml }}
{{- $raw := .Values.enrollment | default dict }}
{{- $pinned := or $e.enabled (dig "caBundle" "configMap" "" $raw) (dig "caBundle" "secret" "" $raw) }}
{{- $rotation := $e.rotation }}
{{- if and (not $rotation) (ne (.Values.grid.peerTrust | default "") "spiffe") }}
{{- $rotation = "Disabled" }}
{{- end }}
{{- if or $e.enabled $e.url }}
local:
  signerEndpoint:
    url: {{ $e.url | quote }}
    {{- if $pinned }}
    caBundleRef:
      {{- include "grid-operator.enrollment.bundleRef" $e.caBundle | trim | nindent 6 }}
    {{- end }}
    {{- if include "grid-operator.enrollment.hasGridCa" . }}
    gridCaBundleRef:
      {{- include "grid-operator.enrollment.bundleRef" $e.gridCaBundle | trim | nindent 6 }}
    {{- end }}
    {{- with $rotation }}
    rotation: {{ . | quote }}
    {{- end }}
  {{- if $e.enabled }}
  inviteSecretRef:
    name: {{ dig "tokenSecretRef" "name" "" $e | quote }}
    key: {{ dig "tokenSecretRef" "key" "token" $e | quote }}
  {{- end }}
{{- end }}
{{- end }}

{{/* Emits "true" or nothing. */}}
{{- define "grid-operator.enrollment.hasGridCa" -}}
{{- $e := include "grid-operator.enrollment.settings" . | fromYaml }}
{{- if or (dig "gridCaBundle" "configMap" "" $e) (dig "gridCaBundle" "secret" "" $e) -}}
true
{{- end -}}
{{- end }}
