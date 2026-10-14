-- Schema for grid enrollment: 0004_enrollment_history.down.sql
ALTER TABLE site_tokens DROP COLUMN IF EXISTS allow_deleted_name;
DROP TABLE IF EXISTS deleted_site_names;
DROP TABLE IF EXISTS enrolled_keys;
