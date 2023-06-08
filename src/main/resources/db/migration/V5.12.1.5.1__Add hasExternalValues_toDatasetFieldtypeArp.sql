-- Note: we name our migration versions by adding a new subversion to the latest Dataverse version.
-- In this case V5.12.1.5 --> V5.12.1.5.1
-- This hopefully won't cause class even with minor DV updates and also for sure not with major DV updates where
-- the migration version should be bumped in the first 2 digits anyway.

ALTER TABLE datasetfieldtypearp ADD COLUMN IF NOT EXISTS hasexternalvalues boolean;
ALTER TABLE datasetfieldtypearp ADD COLUMN IF NOT EXISTS displaynamefield TEXT;
