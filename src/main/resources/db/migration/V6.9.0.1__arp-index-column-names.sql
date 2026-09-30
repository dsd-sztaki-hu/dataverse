-- EclipseLink created fieldtype_id and cedartoken from the Java field names.
-- The entity indexes ask for field_type_id and cedar_token. Rename existing
-- columns so those indexes, and the new @JoinColumn / @Column names, match.
-- A fresh install already has the new names, so only rename when the old
-- column is still present.

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'datasetfieldtypearp' AND column_name = 'fieldtype_id'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'datasetfieldtypearp' AND column_name = 'field_type_id'
    ) THEN
        ALTER TABLE datasetfieldtypearp RENAME COLUMN fieldtype_id TO field_type_id;
    ELSIF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'datasetfieldtypearp' AND column_name = 'fieldtype_id'
    ) AND EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'datasetfieldtypearp' AND column_name = 'field_type_id'
    ) THEN
        -- Deploy DDL can add the new column before this migration runs.
        UPDATE datasetfieldtypearp
           SET field_type_id = fieldtype_id
         WHERE field_type_id IS NULL
           AND fieldtype_id IS NOT NULL;
        ALTER TABLE datasetfieldtypearp DROP COLUMN fieldtype_id;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'authenticateduserarp' AND column_name = 'cedartoken'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'authenticateduserarp' AND column_name = 'cedar_token'
    ) THEN
        ALTER TABLE authenticateduserarp RENAME COLUMN cedartoken TO cedar_token;
    ELSIF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'authenticateduserarp' AND column_name = 'cedartoken'
    ) AND EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'authenticateduserarp' AND column_name = 'cedar_token'
    ) THEN
        UPDATE authenticateduserarp
           SET cedar_token = cedartoken
         WHERE cedar_token IS NULL
           AND cedartoken IS NOT NULL;
        ALTER TABLE authenticateduserarp DROP COLUMN cedartoken;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS index_datasetfieldtypearp_field_type_id
    ON datasetfieldtypearp (field_type_id);
CREATE INDEX IF NOT EXISTS index_metadatablockarp_metadatablock_id
    ON metadatablockarp (metadatablock_id);
CREATE INDEX IF NOT EXISTS index_authenticateduserarp_cedar_token
    ON authenticateduserarp (cedar_token);
