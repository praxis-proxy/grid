-- Schema for grid enrollment: 0003_site_enrollment_renewal.down.sql
DELETE FROM site_enrollments WHERE site_token_id IS NULL;
ALTER TABLE site_enrollments
    DROP CONSTRAINT IF EXISTS site_enrollments_token_or_reserved,
    DROP COLUMN IF EXISTS previous_public_key_sha256,
    DROP COLUMN IF EXISTS renewed_at,
    DROP COLUMN IF EXISTS frozen_at,
    DROP COLUMN IF EXISTS reserved,
    DROP COLUMN IF EXISTS seed_generation,
    DROP COLUMN IF EXISTS epoch_at,
    DROP COLUMN IF EXISTS not_after,
    ALTER COLUMN site_token_id SET NOT NULL;
