-- Schema for grid enrollment: 0005_grid_ca_anchor.up.sql
-- Description: The fingerprint of the grid CA this database's records belong to.
-- The service records it on first start and refuses a different CA after, so a
-- CA minted after its Secrets were deleted cannot split the grid unnoticed.

CREATE TABLE IF NOT EXISTS grid_ca_anchor (
    -- One row: a database belongs to one grid CA.
    id                 BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
    -- Lowercase hex SHA-256 over the CA's public key (SPKI DER).
    fingerprint_sha256 TEXT NOT NULL CHECK (fingerprint_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
