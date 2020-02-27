-- upgrade script to migrate the Koji database schema
-- from version 1.20 to 1.21


BEGIN;

-- merge_mode can not be null
UPDATE tag_external_repos SET merge_mode = 'koji' WHERE merge_mode is NULL;
ALTER TABLE tag_external_repos ALTER COLUMN merge_mode SET NOT NULL;

COMMIT;
