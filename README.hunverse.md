# Hunverse

This Docker Compose stack is made to let the users run Hunverse on their systems. The stack uses the hosted CEDAR registry at https://cedar.schema.researchdata.hu and the self-hosted AROMA UI at `/aroma`.

## Running

Most settings have defaults, so the stack can be started right after you copy `.env.example` to `.env` and set your `ARP_CEDAR_PROXY_API_KEY`. <br/>
Setting the API key is optional, but recommended before first use so `arp-setup` can import CEDAR metadata blocks. <br/>
You can skip it, try the UI, then set the key later and import via the API (see Config endpoints).

```bash
cp .env.example .env
```

If you set the key now, paste the hex only (no `apiKey ` prefix) from your CEDAR profile at https://cedar.schema.researchdata.hu to the `.env` file.

To run the stack:

```bash
docker compose up
```

The Hunverse look is already in the image. See Branding if you want to change banners, CSS, or logos.

Default login:

```
http://localhost:8080
user: dataverseAdmin
pass: admin1
```

To stop the stack:

```bash
docker compose down # stop
docker compose down -v  # stop and delete data
```

## Configure compose.yml

Copy `.env.example` to `.env` and uncomment a line to override it. Defaults below match the `${VAR:-...}` values in `compose.yml`. <br/>
Customizable vars grouped by services are listed below:

### Change before production

These demo values are unsafe on a public or shared host. Set them in `.env`. Remaining hardcoded items must be edited in `compose.yml`.

```bash
DATAVERSE_ADMIN_PASSWORD=admin1  # bootstrap; builtin dataverseAdmin password
DATAVERSE_DB_USER=dataverse  # Postgres user; also used by dataverse
DATAVERSE_DB_PASSWORD=secret  # Postgres password; also used by dataverse
DATAVERSE_CORS_ORIGIN=*  # browser CORS allow-origin
DATAVERSE_PID_FAKE_AUTHORITY=10.5072  # fake DOI authority
DATAVERSE_PID_FAKE_SHOULDER=FK2/  # fake DOI shoulder
```

Changing `DATAVERSE_DB_USER` or `DATAVERSE_DB_PASSWORD` after the first start has no effect unless you recreate the Postgres volume (`docker compose down -v`).

Hardcoded in `compose.yml`:

- Published ports `5432` (Postgres), `8983` (Solr), `8686` (JMX)
- `DATAVERSE_SITEURL` is HTTP via `MACHINE_IP` (no TLS)
- SMTP is MailDev, not a real mail server
- Bootstrap runs in insecure mode (admin API from the host needs no unblock key)

### CEDAR

```bash
ARP_CEDAR_PROXY_API_KEY=  # hex only, from https://cedar.schema.researchdata.hu
CEDAR_IMPORT_FOLDER_ID=https://repo.schema.researchdata.hu/folders/49ba90b3-86ee-45b8-a623-d4a7a7df926c  # public ARP templates folder; change only to import another folder
```

### Dataverse

```bash
DATAVERSE_IMAGE=harbor.sztaki.hu/arp/hunverse:6.9
MACHINE_IP=localhost  # site URL becomes http://${MACHINE_IP}:8080; set a reachable IP if you open Hunverse from another machine
ARP_CEDAR_DOMAIN=schema.researchdata.hu  # hosted schema registry
ARP_CEDAR_PROXY_API_KEY=  # see Recommended before first use
ARP_AROMA_ADDRESS=http://localhost:8080/aroma  # AROMA UI bundled in Dataverse
TERMINOLOGY_URL_BRANCHES=https://terminology.schema.researchdata.hu/bioportal/ontologies/%s/classes/%s/descendants?page=1&pageSize=500
TERMINOLOGY_URL_VALUESETS=https://terminology.schema.researchdata.hu/bioportal/vs-collections/%s/value-sets/%s/values?page=1&pageSize=%s
TERMINOLOGY_URL_ONTOLOGIES=https://terminology.schema.researchdata.hu/bioportal/ontologies/%s/classes?page=1&pageSize=500
ARP_W3ID_BASE=https://w3id.org/arp/dev
ARP_HARVEST_REGISTRY_CHECK_ENABLED=0  # set 1 to notify superusers if DATAVERSE_SITEURL is missing from https://search.researchdata.hu/stats
ARP_LANG_PACKS_UPDATE_ON_START=true  # overlay en_US / hu_HU from GitHub onto /dv/langBundles
ARP_LANG_PACKS_REPO_URL=https://github.com/dsd-sztaki-hu/dataverse-language-packs
ARP_LANG_PACKS_REF=develop-hu-concorda-v6.9
DATAVERSE_DB_USER=dataverse  # must match postgres
DATAVERSE_DB_PASSWORD=secret  # must match postgres
DATAVERSE_CORS_ORIGIN=*  # change before production
DATAVERSE_PID_FAKE_AUTHORITY=10.5072
DATAVERSE_PID_FAKE_SHOULDER=FK2/
```

### Branding

The Hunverse look is already in the Dataverse image. You do not need a local branding folder to run the stack.

To change banners, CSS, or footer stamps, overlay a folder of files on top of the image:

1. Copy the current theme out of the running container:

```bash
docker cp dataverse:/opt/payara/deployments/dataverse/branding ./branding
```

2. Edit files under `./branding`. Keep the same filenames, or update the paths in `branding-config.js`.

3. In `.env`:

```bash
BRANDING_DIR=./branding
```

4. In `compose.yml`, uncomment the two branding volume lines:

```yaml
- ${BRANDING_DIR}:/var/www/dataverse/branding:ro
- ${BRANDING_DIR}:/opt/payara/deployments/dataverse/branding:ro
```

5. Recreate Dataverse and refresh the browser:

```bash
docker compose up -d dataverse
```

CSS and PNGs apply on refresh. If you rename the navbar logo file, set `LOGO_CUSTOMIZATION_FILE` (for example `/branding/mylogo.png`) and run `docker compose up arp-setup`.

| File | Role |
|---|---|
| `custom-stylesheet.css` | Colors and layout |
| `custom-header.html` | Top banner markup |
| `custom-footer.html` | Footer copyright and stamps |
| `branding-config.js` | `topbanner`, `navbarLogo`, `footerstamps` |
| `topbanner_hunverse_002_eng.png` | English header banner |
| `topbanner_hunverse_002.png` | Hungarian header banner |
| `topbanner_hunverse_002_dark425_eng.png` | English navbar logo |
| `topbanner_hunverse_002_dark425.png` | Hungarian navbar logo |
| `hunverse_portal2_btn.png` | Hungarian portal button |
| `hunverse_portal_button_en_001.png` | English portal button |
| `unified_search_btn.png` | Hungarian federated search button |
| `federated_search_button_en_003.png` | English federated search button |

`footerstamps` in `branding-config.js` is an empty list by default. Add `{ src, href, alt, width }` objects to show seals.


### arp-setup

```bash
ARP_CEDAR_DOMAIN=schema.researchdata.hu
ARP_CEDAR_PROXY_API_KEY=  # see Recommended; skips import and arp.cedar.proxyApiKey when empty
ARP_AROMA_ADDRESS=http://localhost:8080/aroma
ARP_W3ID_BASE=https://w3id.org/arp/dev
CEDAR_IMPORT_DV=root  # collection that receives imported CEDAR templates
CEDAR_IMPORT_FOLDER_ID=https://repo.schema.researchdata.hu/folders/49ba90b3-86ee-45b8-a623-d4a7a7df926c
LOGO_CUSTOMIZATION_FILE=/branding/topbanner_hunverse_002_dark425.png  # navbar logo path inside the container
TERMINOLOGY_URL_TEMPLATE=https://terminology.schema.researchdata.hu/bioportal/ontologies/%s/classes/%s/descendants?pageSize=500  # setup-only
TERMINOLOGY_URL_BRANCHES=https://terminology.schema.researchdata.hu/bioportal/ontologies/%s/classes/%s/descendants?page=1&pageSize=500
TERMINOLOGY_URL_VALUESETS=https://terminology.schema.researchdata.hu/bioportal/vs-collections/%s/value-sets/%s/values?page=1&pageSize=%s
TERMINOLOGY_URL_ONTOLOGIES=https://terminology.schema.researchdata.hu/bioportal/ontologies/%s/classes?page=1&pageSize=500
```

### previewers-provider and register-previewers

```bash
MACHINE_IP=localhost  # PREVIEWERS_PROVIDER_URL=http://${MACHINE_IP}:9080
```

### solr-updater

```bash
SOLR_UPDATER_IMAGE=harbor.sztaki.hu/arp/dataverse-solr-updater:6.9
```

### dataverse-rocrate-preview

```bash
ROCRATE_PREVIEW_IMAGE=harbor.sztaki.hu/arp/dataverse-rocrate-preview:6.9
```

### postgres

```bash
DATAVERSE_DB_USER=dataverse  # POSTGRES_USER; must match dataverse
DATAVERSE_DB_PASSWORD=secret  # POSTGRES_PASSWORD; must match dataverse
```

Only applied when the `postgres_data` volume is first created.

## arp-setup

`docker compose up` runs it after Dataverse and `bootstrap` are ready.

It waits for `/api/info/version` and the root dataverse, then PUTs:

- `:FilePIDsEnabled`, `:AllowEnablingFilePIDsPerCollection`
- `arp.w3id.base`, `arp.cedar.domain`, `arp.aroma.address`, `arp.cedar.proxyApiKey`
- `terminology.url.template`, `terminology.url.branches`, `terminology.url.valueSets`, `terminology.url.ontologies`
- `:HeaderCustomizationFile`, `:FooterCustomizationFile`, `:StyleCustomizationFile`, `:LogoCustomizationFile`
- `:Languages`, `:MetadataLanguages` (English + Magyar)

If `ARP_CEDAR_PROXY_API_KEY` and `CEDAR_IMPORT_FOLDER_ID` are set, it also imports the templates from CEDAR via `importTemplatesFromCedarFolder`. If either is empty, settings are still applied and the import is skipped.

Re-run after changing those `.env` values, after a failed first import, or after changing the navbar logo filename:

```bash
docker compose up arp-setup
```

## Config endpoints

Bootstrap runs in insecure mode. Curl from the host needs no unblock key.

### Settings

```bash
curl http://localhost:8080/api/admin/settings
```

```bash
curl -X PUT -d 'http://localhost:8080/aroma' \
  http://localhost:8080/api/admin/settings/arp.aroma.address
```

```bash
curl -X DELETE http://localhost:8080/api/admin/settings/arp.aroma.address
```

### Translations

Overlays `en_US` / `hu_HU` from GitHub onto `/dv/langBundles`. Does not delete CEDAR UUID property files. Omit the JSON body to use the repo/ref from compose / ArpConfig.

```bash
curl -X POST 'http://localhost:8080/api/admin/langPacks/update' \
  -H 'Content-Type: application/json' \
  -d '{"repoUrl":"https://github.com/dsd-sztaki-hu/dataverse-language-packs","ref":"develop-hu-concorda-v6.9"}'
```


### CEDAR templates

Use this if you skipped `ARP_CEDAR_PROXY_API_KEY` on first boot. Store the key, then run the same import `arp-setup` would have run.

```bash
curl -X PUT -d '<hex>' \
  http://localhost:8080/api/admin/settings/arp.cedar.proxyApiKey
```

```bash
curl -X POST 'http://localhost:8080/api/admin/arp/importTemplatesFromCedarFolder' \
  -H 'Content-Type: application/json' \
  -d '{"folderId":"https://repo.schema.researchdata.hu/folders/49ba90b3-86ee-45b8-a623-d4a7a7df926c","dvIdtf":"root"}'
```


## Volumes

Named volumes (persist until `docker compose down -v`):

| Volume | Mount | What |
|---|---|---|
| `dv_data` | `/dv` | uploaded files, langBundles, exporters |
| `dv_secrets` | `/secrets` | secrets |
| `postgres_data` | `/var/lib/postgresql/data` | database |
| `solr_data` | `/var/solr` | Solr index |
| `solr_conf` | Solr template | Solr config copied by `solr_initializer` |

Optional bind mounts (commented out in `compose.yml`; see Branding to overlay a custom look):

```
${BRANDING_DIR} → /var/www/dataverse/branding
${BRANDING_DIR} → /opt/payara/deployments/dataverse/branding
```

tmpfs (wiped on container stop): Dataverse `/dumps` and `/tmp`, smtp `/mail`.

## Ports

| Port | What |
|---|---|
| 8080 | Dataverse |
| 1080 | Mail UI |
| 8983 | Solr |
| 5432 | Postgres |
| 8984 | Solr updater |
| 8985 | RO-Crate preview |
| 9080 | Previewers |

## Images

| Service | Image |
|---|---|
| `dataverse` | `harbor.sztaki.hu/arp/hunverse:6.9` |
| `bootstrap` | `gdcc/configbaker:unstable` |
| `arp-setup` | `gdcc/configbaker:unstable` |
| `dv_initializer` | `gdcc/configbaker:unstable` |
| `solr_initializer` | `gdcc/configbaker:unstable` |
| `postgres` | `postgres:16` |
| `solr` | `solr:9.8.0` |
| `smtp` | `maildev/maildev:2.0.5` |
| `solr-updater` | `harbor.sztaki.hu/arp/dataverse-solr-updater:6.9` |
| `dataverse-rocrate-preview` | `harbor.sztaki.hu/arp/dataverse-rocrate-preview:6.9` |
| `previewers-provider` | `trivadis/dataverse-previewers-provider:latest` |
| `register-previewers` | `trivadis/dataverse-deploy-previewers:latest` |

