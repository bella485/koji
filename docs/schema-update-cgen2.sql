BEGIN;

-- New tables

SELECT statement_timestamp(), 'Creating new tables' as msg;

CREATE TABLE btype (
        id SERIAL NOT NULL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL
) WITHOUT OIDS;

CREATE TABLE build_types (
        build_id INTEGER NOT NULL REFERENCES build(id),
        btype_id INTEGER NOT NULL REFERENCES btype(id),
        PRIMARY KEY (build_id, btype_id)
) WITHOUT OIDS;

-- predefined build types

SELECT statement_timestamp(), 'Adding predefined build types' as msg;
INSERT INTO btype(name) VALUES ('rpm');
INSERT INTO btype(name) VALUES ('maven');
INSERT INTO btype(name) VALUES ('win');
INSERT INTO btype(name) VALUES ('image');

-- new column for archiveinfo

SELECT statement_timestamp(), 'Altering archiveinfo table' as msg;
ALTER TABLE archiveinfo ADD COLUMN btype_id INTEGER REFERENCES btype(id);


-- new component tables
SELECT statement_timestamp(), 'Creating new component tables' as msg;
CREATE TABLE archive_rpm_components AS SELECT image_id, rpm_id from image_listing;
CREATE TABLE archive_components AS SELECT image_id, archive_id from image_archive_listing;
-- doing it this way and fixing up after is *much* faster than creating the empty table
-- and using insert..select to populate

SELECT statement_timestamp(), 'Fixing up component tables, rename columns' as msg;
ALTER TABLE archive_rpm_components RENAME image_id TO archive_id;
ALTER TABLE archive_components RENAME archive_id TO component_id;
ALTER TABLE archive_components RENAME image_id TO archive_id;

SELECT statement_timestamp(), 'Fixing up component tables, adding constraints' as msg;
ALTER TABLE archive_rpm_components ADD CONSTRAINT archive_rpm_components_archive_fk FOREIGN KEY (archive_id) REFERENCES archiveinfo(id);
ALTER TABLE archive_rpm_components ADD CONSTRAINT archive_rpm_components_rpm_fk FOREIGN KEY (rpm_id) REFERENCES rpminfo(id);
ALTER TABLE archive_rpm_components ADD CONSTRAINT archive_rpm_components_unique UNIQUE (archive_id, rpm_id);
ALTER TABLE archive_components ADD CONSTRAINT archive_components_archive_fk FOREIGN KEY (archive_id) REFERENCES archiveinfo(id);
ALTER TABLE archive_components ADD CONSTRAINT archive_components_rpm_fk FOREIGN KEY (component_id) REFERENCES archiveinfo(id);
ALTER TABLE archive_components ADD CONSTRAINT archive_components_unique UNIQUE (archive_id, component_id);

SELECT statement_timestamp(), 'Adding component table indexes' as msg;
CREATE INDEX rpm_components_idx on archive_rpm_components(rpm_id);
CREATE INDEX archive_components_idx on archive_components(component_id);


-- image_listing and image_archive_listing are no longer used


COMMIT;

