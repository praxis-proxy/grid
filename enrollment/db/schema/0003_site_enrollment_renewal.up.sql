-- Schema for grid enrollment: 0003_site_enrollment_renewal.up.sql
-- Description: A site renews its identity with its current key. The record keeps
-- the key it replaced so a renewal whose response was lost can retry, and is frozen
-- when that replaced key asks for any other. A reserved name, issued by bootstrap
-- with no site token, gets its record from a seed bootstrap signs with the CA key.

ALTER TABLE site_enrollments
    ADD COLUMN IF NOT EXISTS previous_public_key_sha256 TEXT,
    ADD COLUMN IF NOT EXISTS renewed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS frozen_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS reserved BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS seed_generation BIGINT,
    ADD COLUMN IF NOT EXISTS epoch_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS not_after TIMESTAMPTZ,
    ALTER COLUMN site_token_id DROP NOT NULL;

-- When this incarnation of the record began: its insert, or a seed reset. A leaf
-- issued before it comes from an enrollment a recovery replaced.
UPDATE site_enrollments SET epoch_at = issued_at WHERE epoch_at IS NULL;
ALTER TABLE site_enrollments
    ALTER COLUMN epoch_at SET DEFAULT NOW(),
    ALTER COLUMN epoch_at SET NOT NULL;

-- Only a reserved name's record may lack the token that enrolled it.
DO $$
BEGIN
    ALTER TABLE site_enrollments
        ADD CONSTRAINT site_enrollments_token_or_reserved
        CHECK (site_token_id IS NOT NULL OR reserved);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END
$$;
