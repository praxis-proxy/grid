-- Schema for grid enrollment: 0001_create_site_tokens.up.sql
-- Description: Single-use tokens an operator mints so a site can enroll under a
-- name the operator pinned. The site never chooses its own name.

CREATE TABLE IF NOT EXISTS site_tokens (
    id           UUID PRIMARY KEY,
    -- The token is kept as a digest. A timing difference reveals nothing about a
    -- valid token, and the table is not a list of usable credentials at rest.
    token_sha256 TEXT NOT NULL,
    -- The pinned name. NOT NULL, so the database, not the code, guarantees every
    -- token pins a name and enroll never falls back to a caller-supplied one.
    site_name    TEXT NOT NULL,
    grid_network_ref TEXT NOT NULL,
    issued_by    TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at   TIMESTAMPTZ NOT NULL,
    -- Set when redeemed. A token buys one enrollment, not a standing right.
    redeemed_at  TIMESTAMPTZ,
    redeemed_by  UUID,

    CONSTRAINT site_tokens_redeemed_together
        CHECK ((redeemed_at IS NULL) = (redeemed_by IS NULL))
);

-- Redemption looks a token up by digest on every enrollment.
CREATE UNIQUE INDEX IF NOT EXISTS idx_site_tokens_token
    ON site_tokens (token_sha256);

-- Operators list what is outstanding.
CREATE INDEX IF NOT EXISTS idx_site_tokens_created
    ON site_tokens (created_at DESC);
