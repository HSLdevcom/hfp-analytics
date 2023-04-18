CREATE SCHEMA importer;
COMMENT ON SCHEMA importer IS
'Contains data of blob importing status.';

-- Import statuses.
CREATE TABLE importer.import_status (
  status        text          PRIMARY KEY
);
COMMENT ON TABLE importer.import_status IS
'Dimension table for allowed status values.';
INSERT INTO importer.import_status (status)
VALUES ('not started'), ('pending'), ('importing'), ('failed'), ('imported');



-- HFP blobs.
-- Importer keeps track of imported blobs on this table
CREATE TABLE importer.blob (
  name              text          PRIMARY KEY,
  type              text,
  min_oday          date,
  max_oday          date,
  min_tst           timestamptz,
  max_tst           timestamptz,
  row_count         integer,
  invalid           boolean       DEFAULT FALSE,
  listed_at         timestamptz   DEFAULT NOW(),
  covered_by_import boolean       DEFAULT FALSE,
  import_status     text REFERENCES importer.import_status(status) DEFAULT 'not started',
  import_started    timestamptz   DEFAULT NULL,
  import_finished   timestamptz   DEFAULT NULL
);
COMMENT ON TABLE importer.blob IS
'Blobs found by imported on Azure Storage';
COMMENT ON COLUMN importer.blob.name is
'Name of the blob.';
COMMENT ON COLUMN importer.blob.type IS
'Type of events contained by the blob.';
COMMENT ON COLUMN importer.blob.min_oday IS
'Minimum operating day in the data claimed by the tags of the blob.';
COMMENT ON COLUMN importer.blob.max_oday IS
'Maximum operating day in the data claimed by the tags of the blob.';
COMMENT ON COLUMN importer.blob.min_tst IS
'Minimum timestamp in the data claimed by the tags of the blob.';
COMMENT ON COLUMN importer.blob.max_tst IS
'Maximum timestamp in the data claimed by the tags of the blob.';
COMMENT ON COLUMN importer.blob.invalid IS
'If the blob was marked by Transitdata to contain invalid data.';
COMMENT ON COLUMN importer.blob.listed_at IS
'When the blob was recognized by the importer.';
COMMENT ON COLUMN importer.blob.covered_by_import IS
'Flag to mark that the blob should be imported.';
COMMENT ON COLUMN importer.blob.import_status IS
'Status of the import process.';
COMMENT ON COLUMN importer.blob.import_started IS
'When the blob import process was started.';
COMMENT ON COLUMN importer.blob.import_started IS
'When the blob import process was finished.';