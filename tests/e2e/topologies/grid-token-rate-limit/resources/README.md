# Kubernetes Resources

This directory owns Kubernetes resources that are not emitted directly by the
two Helm charts. Keep reusable resources under `common/` and site-specific
resources under `west/`, `central/`, and `east/`.

Resources must preserve separate consumer and provider identities and prevent
consumer workloads from reaching private inference endpoints directly. Secret
manifests and credential values must never be committed.
