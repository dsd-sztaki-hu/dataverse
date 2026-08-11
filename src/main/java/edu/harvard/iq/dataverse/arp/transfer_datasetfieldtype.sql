--
-- Transfer instance data from one DatasetFieldType to another, then delete the original type.
--
-- Aimed at geospatial-style duplicates where both types already exist after a TSV reload
-- (e.g. northLongitude / southLongitude vs northLatitude / southLatitude). The two types
-- are the same field with different names/URLs; instance values on the original type are
-- remounted to the new type. When the same compound (or version/template) scope already
-- has the new type, the new field is kept and the original field is removed.
--
-- Usage:
--   SELECT transfer_datasetfieldtype('northLongitude', 'northLatitude');
--   SELECT transfer_datasetfieldtype('southLongitude', 'southLatitude');
--
-- Pre-flight: inspect instance rows that would collide before running:
--
--   SELECT old_df.id AS original_field_id,
--          new_df.id AS new_field_id,
--          old_df.parentdatasetfieldcompoundvalue_id,
--          old_df.datasetversion_id,
--          old_df.template_id,
--          old_dft.name AS original_type,
--          new_dft.name AS new_type
--   FROM datasetfield old_df
--   JOIN datasetfieldtype old_dft ON old_dft.id = old_df.datasetfieldtype_id
--   JOIN datasetfield new_df ON new_df.datasetfieldtype_id = (
--           SELECT id FROM datasetfieldtype WHERE name = 'northLatitude')
--       AND (
--           (old_df.parentdatasetfieldcompoundvalue_id IS NOT NULL
--            AND old_df.parentdatasetfieldcompoundvalue_id = new_df.parentdatasetfieldcompoundvalue_id)
--           OR (old_df.parentdatasetfieldcompoundvalue_id IS NULL
--               AND old_df.datasetversion_id IS NOT NULL
--               AND old_df.datasetversion_id = new_df.datasetversion_id)
--           OR (old_df.parentdatasetfieldcompoundvalue_id IS NULL
--               AND old_df.datasetversion_id IS NULL
--               AND old_df.template_id IS NOT NULL
--               AND old_df.template_id = new_df.template_id)
--       )
--   WHERE old_dft.name = 'northLongitude';
--

CREATE OR REPLACE FUNCTION transfer_datasetfieldtype(
    p_original_name VARCHAR,
    p_new_name VARCHAR
)
    RETURNS void AS $$
DECLARE
    v_original_id BIGINT;
    v_new_id BIGINT;
    v_original_mdb_id BIGINT;
    v_new_mdb_id BIGINT;
    v_remounted INTEGER := 0;
    v_dropped INTEGER := 0;
    r_field RECORD;
    v_collision BOOLEAN;
BEGIN
    SELECT id, metadatablock_id
    INTO v_original_id, v_original_mdb_id
    FROM public.datasetfieldtype
    WHERE name = p_original_name;

    IF v_original_id IS NULL THEN
        RAISE EXCEPTION 'DatasetFieldType with name % not found', p_original_name;
    END IF;

    SELECT id, metadatablock_id
    INTO v_new_id, v_new_mdb_id
    FROM public.datasetfieldtype
    WHERE name = p_new_name;

    IF v_new_id IS NULL THEN
        RAISE EXCEPTION 'DatasetFieldType with name % not found', p_new_name;
    END IF;

    IF v_original_id = v_new_id THEN
        RAISE EXCEPTION 'Original and new DatasetFieldType resolve to the same id %', v_original_id;
    END IF;

    IF v_original_mdb_id IS DISTINCT FROM v_new_mdb_id THEN
        RAISE EXCEPTION
            'DatasetFieldTypes % and % belong to different metadata blocks (% vs %)',
            p_original_name, p_new_name, v_original_mdb_id, v_new_mdb_id;
    END IF;

    -- Remount or drop instance fields of the original type
    FOR r_field IN
        SELECT id,
               parentdatasetfieldcompoundvalue_id,
               datasetversion_id,
               template_id
        FROM public.datasetfield
        WHERE datasetfieldtype_id = v_original_id
    LOOP
        v_collision := FALSE;

        IF r_field.parentdatasetfieldcompoundvalue_id IS NOT NULL THEN
            SELECT EXISTS (
                SELECT 1
                FROM public.datasetfield
                WHERE datasetfieldtype_id = v_new_id
                  AND parentdatasetfieldcompoundvalue_id = r_field.parentdatasetfieldcompoundvalue_id
            ) INTO v_collision;
        ELSIF r_field.datasetversion_id IS NOT NULL THEN
            SELECT EXISTS (
                SELECT 1
                FROM public.datasetfield
                WHERE datasetfieldtype_id = v_new_id
                  AND parentdatasetfieldcompoundvalue_id IS NULL
                  AND datasetversion_id = r_field.datasetversion_id
            ) INTO v_collision;
        ELSIF r_field.template_id IS NOT NULL THEN
            SELECT EXISTS (
                SELECT 1
                FROM public.datasetfield
                WHERE datasetfieldtype_id = v_new_id
                  AND parentdatasetfieldcompoundvalue_id IS NULL
                  AND datasetversion_id IS NULL
                  AND template_id = r_field.template_id
            ) INTO v_collision;
        END IF;

        IF v_collision THEN
            -- Keep the new field; drop the original field and its text values
            DELETE FROM public.datasetfieldvalue
            WHERE datasetfield_id = r_field.id;

            DELETE FROM public.datasetfield
            WHERE id = r_field.id;

            v_dropped := v_dropped + 1;
        ELSE
            UPDATE public.datasetfield
            SET datasetfieldtype_id = v_new_id
            WHERE id = r_field.id;

            v_remounted := v_remounted + 1;
        END IF;
    END LOOP;

    -- Clear remaining type-level refs so the original DatasetFieldType row can be deleted
    DELETE FROM public.dataversefacet
    WHERE datasetfieldtype_id = v_original_id;

    DELETE FROM public.dataversefieldtypeinputlevel
    WHERE datasetfieldtype_id = v_original_id;

    DELETE FROM public.datasetfielddefaultvalue
    WHERE datasetfield_id = v_original_id;

    UPDATE public.dataset
    SET citationdatedatasetfieldtype_id = NULL
    WHERE citationdatedatasetfieldtype_id = v_original_id;

    -- ARP / override rows must be removed before datasetfieldtype
    DELETE FROM public.datasetfieldtypearp
    WHERE fieldtype_id = v_original_id;

    DELETE FROM public.datasetfieldtypeoverride
    WHERE original_id = v_original_id;

    DELETE FROM public.datasetfieldtype
    WHERE id = v_original_id;

    RAISE NOTICE
        'Transferred % -> %: remounted % field(s), dropped % colliding field(s), deleted type id %',
        p_original_name, p_new_name, v_remounted, v_dropped, v_original_id;
END;
$$ LANGUAGE plpgsql;
