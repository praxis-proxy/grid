# Praxis Configuration

This directory owns the consumer-gateway and provider-gateway Praxis
configuration used by the combined-site demo.

Consumer configuration must contain the Grid-managed routing overlay mount and
must not contain provider credentials. Provider configuration must enforce
mTLS peer identity, provider-route authorization, and final-hop credential
injection before forwarding to a private inference endpoint.

Site-specific addresses and identities should be rendered from structured
inputs. Do not maintain three hand-edited copies of otherwise identical Praxis
configuration.
