-- Schema for grid enrollment: 0002_create_site_enrollments.up.sql
-- Description: The minimal audit of an issued identity. Not a lifecycle: there
-- is no pending or denied state. Proof of issuance, and the future feed for the
-- GridSite projection.

CREATE TABLE IF NOT EXISTS site_enrollments (
    id                UUID PRIMARY KEY,
    site_token_id     UUID NOT NULL,
    -- One issued identity per name. A second enrollment for a held name is
    -- refused rather than minting a colliding identity.
    site_name         TEXT NOT NULL,
    public_key_sha256 TEXT NOT NULL,
    spiffe_id         TEXT NOT NULL,
    issued_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_site_enrollments_name
    ON site_enrollments (site_name);
