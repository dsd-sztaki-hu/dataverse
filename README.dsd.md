# branch: datasetfieldtype-refact

## Trial #2 - datasetfieldtype_override table

```sql
do
$$
    declare
my_mdb_id   integer;
        title_id    integer;
        subtitle_id integer;
        alttitle_id integer;
begin
INSERT INTO public.metadatablock (displayname, name, namespaceuri, owner_id)
VALUES (''ARP Metadata'', ''arp_metadata'', ''https://arp.org/schema/citation'', null)
    returning id into my_mdb_id;

INSERT INTO public.datasetfieldtype (id, advancedsearchfieldtype, allowcontrolledvocabulary, allowmultiples,
                                     description, displayformat, displayoncreate, displayorder, facetable,
                                     fieldtype, name, required, title, uri, validationformat, watermark,
                                     metadatablock_id, parentdatasetfieldtype_id)
VALUES (1, true, false, false, ''ARP Location description'', '''', true, 0, false, ''TEXT'', ''arpLocation'',
        true, ''ARP Location'', ''http://ar.org/terms/location'', null, '''', my_mdb_id, null);

select id from datasetfieldtype where name = ''title'' into title_id;
select id from datasetfieldtype where name = ''subtitle'' into subtitle_id;
select id from datasetfieldtype where name = ''alternativeTitle'' into alttitle_id;

-- Same as title but with different localname
INSERT INTO public.datasetfieldtypeoverride (metadatablock_id, localname, title, description, watermark,
                                             displayorder, required)
VALUES (my_mdb_id, ''arp_title'', ''Title'', ''The main title of the Dataset'', null, null, null);


-- Same as subtitle but with different local_name and updated Title
INSERT INTO public.datasetfieldtypeoverride (metadatablock_id, localname, title, description, watermark,
                                             displayorder, required)
VALUES (my_mdb_id, ''arp_subtitle'', ''ARP Subtitle'',
        ''ARP A secondary title that amplifies or states certain limitations on the main title'', null, null,
        null);

-- Same as alternativeTitle with no local_name
INSERT INTO public.datasetfieldtypeoverride (metadatablock_id, localname, title, description, watermark,
                                             displayorder, required)
VALUES (my_mdb_id, null, ''ARP Alt title'',
        ''ARP Either 1) a title commonly used to refer to the Dataset or 2) an abbreviation of the main title'',  null, null, null);

end;
$$;
do
$$
    declare
my_mdb_id   integer;
        title_id    integer;
        subtitle_id integer;
        alttitle_id integer;
begin
INSERT INTO public.metadatablock (displayname, name, namespaceuri, owner_id)
VALUES ('ARP Metadata', 'arp_metadata', 'https://arp.org/schema/citation', null)
    returning id into my_mdb_id;

INSERT INTO public.datasetfieldtype (id, advancedsearchfieldtype, allowcontrolledvocabulary, allowmultiples,
                                     description, displayformat, displayoncreate, displayorder, facetable,
                                     fieldtype, name, required, title, uri, validationformat, watermark,
                                     metadatablock_id, parentdatasetfieldtype_id)
VALUES (1, true, false, false, ''ARP Location description'', '''', true, 0, false, ''TEXT'', ''arpLocation'',
        true, ''ARP Location'', ''http://ar.org/terms/location'', null, '''', my_mdb_id, null);

select id from datasetfieldtype where name = ''title'' into title_id;
select id from datasetfieldtype where name = ''subtitle'' into subtitle_id;
select id from datasetfieldtype where name = ''alternativeTitle'' into alttitle_id;

-- Same as title but with different localname
INSERT INTO public.datasetfieldtypeoverride (metadatablock_id, localname, title, description, watermark,
                                             displayorder, required)
VALUES (my_mdb_id, ''arp_title'', ''Title'', ''The main title of the Dataset'', null, null, null);


-- Same as subtitle but with different local_name and updated Title
INSERT INTO public.datasetfieldtypeoverride (metadatablock_id, localname, title, description, watermark,
                                             displayorder, required)
VALUES (my_mdb_id, ''arp_subtitle'', ''ARP Subtitle'',
        ''ARP A secondary title that amplifies or states certain limitations on the main title'', null, null,
        null);

-- Same as alternativeTitle with no local_name
INSERT INTO public.datasetfieldtypeoverride (metadatablock_id, localname, title, description, watermark,
                                             displayorder, required)
VALUES (my_mdb_id, null, ''ARP Alt title'',
        ''ARP Either 1) a title commonly used to refer to the Dataset or 2) an abbreviation of the main title'',  null, null, null);

end;
$$;
do
$$
    declare
my_mdb_id   integer;
        title_id    integer;
        subtitle_id integer;
        alttitle_id integer;
begin
INSERT INTO public.metadatablock (displayname, name, namespaceuri, owner_id)
VALUES ('ARP Metadata', 'arp_metadata', 'https://arp.org/schema/citation', null)
    returning id into my_mdb_id;

INSERT INTO public.datasetfieldtype (id, advancedsearchfieldtype, allowcontrolledvocabulary, allowmultiples,
                                     description, displayformat, displayoncreate, displayorder, facetable,
                                     fieldtype, name, required, title, uri, validationformat, watermark,
                                     metadatablock_id, parentdatasetfieldtype_id)
VALUES (1, true, false, false, 'ARP Location description', '', true, 0, false, 'TEXT', 'arpLocation',
        true, 'ARP Location', 'http://ar.org/terms/location', null, '', my_mdb_id, null);

select id from datasetfieldtype where name = 'title' into title_id;
select id from datasetfieldtype where name = 'subtitle' into subtitle_id;
select id from datasetfieldtype where name = 'alternativeTitle' into alttitle_id;

-- Same as title but with different localname
INSERT INTO public.datasetfieldtypeoverride (metadatablock_id, localname, title, description, watermark,
                                             displayorder, required)
VALUES (my_mdb_id, 'arp_title', 'Title', 'The main title of the Dataset', null, null, null);


-- Same as subtitle but with different local_name and updated Title
INSERT INTO public.datasetfieldtypeoverride (metadatablock_id, localname, title, description, watermark,
                                             displayorder, required)
VALUES (my_mdb_id, 'arp_subtitle', 'ARP Subtitle',
        'ARP A secondary title that amplifies or states certain limitations on the main title', null, null,
        null);
do
$$
    declare
my_mdb_id   integer;
        title_id    integer;
        subtitle_id integer;
        alttitle_id integer;
begin
INSERT INTO public.metadatablock (displayname, name, namespaceuri, owner_id)
VALUES ('ARP Metadata', 'arp_metadata', 'https://arp.org/schema/citation', null)
    returning id into my_mdb_id;

INSERT INTO public.datasetfieldtype (advancedsearchfieldtype, allowcontrolledvocabulary, allowmultiples,
                                     description, displayformat, displayoncreate, displayorder, facetable,
                                     fieldtype, name, required, title, uri, validationformat, watermark,
                                     metadatablock_id, parentdatasetfieldtype_id)
VALUES (true, false, false, 'ARP Location description', '', true, 0, false, 'TEXT', 'arpLocation',
        true, 'ARP Location', 'http://ar.org/terms/location', null, '', my_mdb_id, null);

select id from datasetfieldtype where name = 'title' into title_id;
select id from datasetfieldtype where name = 'subtitle' into subtitle_id;
select id from datasetfieldtype where name = 'alternativeTitle' into alttitle_id;

-- Same as title but with different localname
INSERT INTO public.datasetfieldtypeoverride (metadatablock_id, original_id, localname, title, description, watermark,
                                             displayorder, required)
VALUES (my_mdb_id, 1, 'arp_title', 'Title', 'The main title of the Dataset', null, null, null);


-- Same as subtitle but with different local_name and updated Title
INSERT INTO public.datasetfieldtypeoverride (metadatablock_id, original_id, localname, title, description, watermark,
                                             displayorder, required)
VALUES (my_mdb_id, 2, 'arp_subtitle', 'ARP Subtitle',
        'ARP A secondary title that amplifies or states certain limitations on the main title', null, null,
        null);

-- Same as alternativeTitle with no local_name
INSERT INTO public.datasetfieldtypeoverride (metadatablock_id, original_id, localname, title, description, watermark,
                                             displayorder, required)
VALUES (my_mdb_id, 3, null, 'ARP Alt title',
        'ARP Either 1) a title commonly used to refer to the Dataset or 2) an abbreviation of the main title',  null, null, null);

end;
$$;

-- Same as alternativeTitle with no local_name
INSERT INTO public.datasetfieldtypeoverride (metadatablock_id, localname, title, description, watermark,
                                             displayorder, required)
VALUES (my_mdb_id, null, 'ARP Alt title',
        'ARP Either 1) a title commonly used to refer to the Dataset or 2) an abbreviation of the main title',  null, null, null);

end;
$$;



```


## Trial #1 - Allow multiple datasetfieldtype with the same name - FAIL

Unfortunately many things depend on the assumption that `name` uniquely identifies a datasetfieldtype. While it may be possible to work around all the places where this assumption is currently made, it just too much effort and too much modification to existing code. 


```sql
ALTER TABLE datasetfieldtype DROP CONSTRAINT datasetfieldtype_name_key;
ALTER TABLE datasetfieldtype ADD COLUMN IF NOT EXISTS local_name text;
```
This branch is used to implement our updates to the Dataverse MDB handling allowing the use of the same DatasetFieldType in multiple MDB-s.

`src/main/resources/db/migration/V5.12.1.1__datasetfieldtype_refact.sql`

updates the database as necessary.

This is an example MDB using shared DatasetFieldType-s

```sql
do $$
    declare
my_mdb_id integer;
begin
INSERT INTO public.metadatablock (displayname, name, namespaceuri, owner_id) VALUES ('ARP Metadata', 'arp_metadata', 'https://arp.org/schema/citation', null) returning id into my_mdb_id;

-- Same as title but with different local_name
INSERT INTO public.datasetfieldtype (advancedsearchfieldtype, allowcontrolledvocabulary, allowmultiples, description, displayformat, displayoncreate, displayorder, facetable, fieldtype, name, required, title, uri, validationformat, watermark, metadatablock_id, parentdatasetfieldtype_id, local_name) VALUES (true, false, false, 'The main title of the Dataset', '', true, 0, false, 'TEXT', 'title', true, 'Title', 'http://purl.org/dc/terms/title', null, '', my_mdb_id, null, 'arp_title');

-- Same as subtitle but with different local_name and updated Title
INSERT INTO public.datasetfieldtype (advancedsearchfieldtype, allowcontrolledvocabulary, allowmultiples, description, displayformat, displayoncreate, displayorder, facetable, fieldtype, name, required, title, uri, validationformat, watermark, metadatablock_id, parentdatasetfieldtype_id, local_name) VALUES (false, false, false, 'ARP A secondary title that amplifies or states certain limitations on the main title', '', false, 1, false, 'TEXT', 'subtitle', false, 'ARP Subtitle', null, null, '', my_mdb_id, null, 'arp_subtitle');

-- Same as alternativeTitle with no local_name
INSERT INTO public.datasetfieldtype (advancedsearchfieldtype, allowcontrolledvocabulary, allowmultiples, description, displayformat, displayoncreate, displayorder, facetable, fieldtype, name, required, title, uri, validationformat, watermark, metadatablock_id, parentdatasetfieldtype_id, local_name) VALUES (false, false, false, 'ARP Either 1) a title commonly used to refer to the Dataset or 2) an abbreviation of the main title', '', false, 2, false, 'TEXT', 'alternativeTitle', false, 'Alternative Title', 'http://purl.org/dc/terms/alternative', null, '', my_mdb_id, null, null);

end; $$;


```
