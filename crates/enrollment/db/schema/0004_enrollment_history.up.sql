-- Schema for grid enrollment: 0004_enrollment_history.up.sql
-- Description: What deleting an enrollment does not erase. Every key the signer
-- has enrolled stays refused, so a deleted site's key cannot enroll again under
-- any name. A deleted name is recorded, so enrolling it again takes a token that
-- opts in.

CREATE TABLE IF NOT EXISTS enrolled_keys (
    -- The primary key, not the code, refuses a reused key under any race.
    public_key_sha256 TEXT PRIMARY KEY,
    site_name         TEXT NOT NULL,
    enrolled_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS deleted_site_names (
    site_name  TEXT PRIMARY KEY,
    deleted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Set at mint by a grid-admin who means to enroll a deleted name again.
ALTER TABLE site_tokens
    ADD COLUMN IF NOT EXISTS allow_deleted_name BOOLEAN NOT NULL DEFAULT FALSE;

-- Keys enrolled before the history existed.
INSERT INTO enrolled_keys (public_key_sha256, site_name, enrolled_at)
SELECT public_key_sha256, site_name, issued_at FROM site_enrollments
ON CONFLICT DO NOTHING;
INSERT INTO enrolled_keys (public_key_sha256, site_name, enrolled_at)
SELECT previous_public_key_sha256, site_name, issued_at FROM site_enrollments
 WHERE previous_public_key_sha256 IS NOT NULL
ON CONFLICT DO NOTHING;

-- Names deleted before the history existed: a redeemed token with no enrollment left.
INSERT INTO deleted_site_names (site_name)
SELECT DISTINCT t.site_name FROM site_tokens t
 WHERE t.redeemed_at IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM site_enrollments e WHERE e.site_name = t.site_name)
ON CONFLICT DO NOTHING;
