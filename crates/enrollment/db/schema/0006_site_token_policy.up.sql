ALTER TABLE site_tokens
    ADD COLUMN IF NOT EXISTS public_key_sha256 TEXT;
