--
-- This script can be used to delete a Metadatablock and all the field values set based on this MDB.
--
-- delete_metadatablock_by_name: delete MDB by name
-- delete_metadatablock_by_id: delete MDB by ID
--

CREATE OR REPLACE FUNCTION delete_metadatablock_by_id(p_metadatablock_id INTEGER)
    RETURNS void AS $$
DECLARE
r_id RECORD;
BEGIN
    -- Create a temporary table to store the IDs
    CREATE TEMP TABLE temp_ids (id INTEGER);

    -- Insert IDs into the temporary table for records that will have parentdatasetfield_id set to NULL
    INSERT INTO temp_ids (id)
    SELECT id FROM public.datasetfieldcompoundvalue
    WHERE parentdatasetfield_id IN
          (SELECT id FROM public.datasetfield WHERE datasetfieldtype_id IN
                                                    (SELECT id FROM public.datasetfieldtype WHERE metadatablock_id = p_metadatablock_id));

    -- Update datasetfieldcompoundvalue to set parentdatasetfield_id to NULL for the stored IDs
    UPDATE public.datasetfieldcompoundvalue
    SET parentdatasetfield_id = NULL
    WHERE id IN (SELECT id FROM temp_ids);

    -- Update or delete datasetfield records that reference the compound values to be deleted
    -- Choose one of the following based on your requirements:
    UPDATE public.datasetfield SET parentdatasetfieldcompoundvalue_id = NULL WHERE parentdatasetfieldcompoundvalue_id IN (SELECT id FROM temp_ids);

    -- Iterate over the IDs and print them for debugging
    FOR r_id IN SELECT id FROM temp_ids
                                   LOOP
        RAISE NOTICE 'ID to be deleted from datasetfieldcompoundvalue: %', r_id.id;
    END LOOP;

        -- Delete from datasetfieldcompoundvalue using the stored IDs
    DELETE FROM public.datasetfieldcompoundvalue
    WHERE id IN (SELECT id FROM temp_ids);

    -- Delete from datasetfieldvalue which depends on datasetfield
    DELETE FROM public.datasetfieldvalue WHERE datasetfield_id IN
                                               (SELECT id FROM public.datasetfield WHERE datasetfieldtype_id IN
                                                                                         (SELECT id FROM public.datasetfieldtype WHERE metadatablock_id = p_metadatablock_id));

    -- Then delete from datasetfield
    DELETE FROM public.datasetfield WHERE datasetfieldtype_id IN
                                          (SELECT id FROM public.datasetfieldtype WHERE metadatablock_id = p_metadatablock_id);

    -- Delete from datasetfieldtypearp and datasetfieldtypeoverride which depend on datasetfieldtype
    DELETE FROM public.datasetfieldtypearp WHERE fieldtype_id IN
                                                 (SELECT id FROM public.datasetfieldtype WHERE metadatablock_id = p_metadatablock_id);

    DELETE FROM public.datasetfieldtypeoverride WHERE metadatablock_id = p_metadatablock_id;

    -- Delete from datasetfieldtype
    DELETE FROM public.datasetfieldtype WHERE metadatablock_id = p_metadatablock_id;

    -- Delete from metadatablockarp and dataverse_metadatablock which depend on metadatablock
    DELETE FROM public.metadatablockarp WHERE metadatablock_id = p_metadatablock_id;
    DELETE FROM public.dataverse_metadatablock WHERE metadatablocks_id = p_metadatablock_id;

    -- Finally, delete from the metadatablock table
    DELETE FROM public.metadatablock WHERE id = p_metadatablock_id;

-- Drop the temporary table
DROP TABLE temp_ids;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION delete_metadatablock_by_name(p_metadatablock_name VARCHAR)
    RETURNS void AS $$
DECLARE
v_metadatablock_id INTEGER;
BEGIN
    -- Get the metadatablock ID from the provided name
    SELECT id INTO v_metadatablock_id FROM public.metadatablock WHERE name = p_metadatablock_name;

    -- Check if the metadatablock exists
    IF v_metadatablock_id IS NULL THEN
            RAISE EXCEPTION 'Metadatablock with name % not found', p_metadatablock_name;
    ELSE
            -- Call the function to delete by ID
            PERFORM delete_metadatablock_by_id(v_metadatablock_id);
    END IF;
END;
$$ LANGUAGE plpgsql;

SELECT delete_metadatablock_by_name('demo-dv-schema');
