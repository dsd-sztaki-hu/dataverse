# Hunverse

[English](README.hunverse.md) · [Magyar](README.hunverse.hu.md)

<img src="src/main/docker/branding/hunverse-readme-logo.png" alt="Hunverse" width="2432">

Hunverse — a magyar Dataverse.

Az Adatrepozitórium Platform (ARP) projektben a Dataverse kutatási adarepizitórum szoftverére alapuló szolgáltatási környezetet építettünk. Ennek középpontjában a Dataverse egy saját fejlesztésekkel kibővített, magyarosított változata áll, amit egyéb szolgáltatásokkal egészítettünk ki: séma regiszterrel, RO-Crate szerkesztővel, és közös keresővel.

A Hunverse ennek a kibővített Dataverse szoftvernek a magyar kutatói közösség számára készült nyílt forráskódú kiadása.

Ez mindenben megegyezik az ARP-ban is futó példánnyal, viszont most minden kutatónak és intézménynek lehetővé válik ennek telepítése és használata oly módon, hogy alapból megkapják az integrációt az ARP szolgáltatásaival. Vagyis, minden Hunverse felhasználó ugyanazon séma regiszterben tárolt sémával tud dolgozni, azokat meg tudják  osztani egymással, az ARP-ban megismert módon tudják az adataikat RO-Crate segítségével - akár fájl szinten is - még részletesebben metaadatolni, valamint egy adott Hunverse installációban keletkező adatcsomagok automatikusan kereshetővé válnak az ARP Közös Keresőben, ami a magyar kutatási adatok federált gyűjtőhelye.

A Hunverse minden intézmény számára testreszabható mind a kinézetét, mind egyes szolgáltatásait tekintve a Dataverse által alapból nyújtott lehetőségek segítségével. Így saját autentikáció, perzisztens azonosító szolgáltatás, vagy egyéb kényelmi kiegészíthetők telepíthetők a Hunversebe igény szerint.

Az ARP projekt folyamatosan biztosítja a Hunverse frissítését az újabb és újabb Dataverse kiadásoknak megfelelően.


Ez a Docker Compose azért készült, hogy a Hunverse futtatható legyen a  felhasználók saját környezetben. A Hunverse a https://cedar.schema.researchdata.hu címen üzemeltetett CEDAR-regisztert használja, az AROMA felületet pedig helyben, az `/aroma` útvonalon szolgálja ki.

## Komponensek

A `hunverse-compose.yml` tizenkét szolgáltatást indít egy Docker-hálózaton. Az ábra balról jobbra olvasandó: az egyszer lefutó feladatok, a Dataverse alkalmazás, majd a vele együtt tovább futó szolgáltatások.

<img src="hunverse-compose.png" alt="Hunverse Compose. Öt egyszeri feladat — dv_initializer, solr_initializer, bootstrap, arp-setup és register-previewers — készíti elő a dataverse-t (Hunverse, 8080-as port). Mellette fut a postgres :5432, a solr :8983, a solr-updater :8984, az smtp :1080, a dataverse-rocrate-preview :8985 és a previewers-provider :9080." width="1440">

**dataverse** a Hunverse alkalmazás. A repozitórium felülete és API-ja a 8080-as portot használja, a JMX a 8686-ost. Az AROMA, az RO-Crate-szerkesztő, ugyanebből az alkalmazásból érhető el a `/aroma` útvonalon. A feltöltött fájlok és a nyelvi csomagok a `dv_data` köteten tárolódnak.

**postgres** a PostgreSQL 16 a 5432-es porton. Gyűjteményeket, adatcsomagokat, felhasználókat és beállításokat tárol.

**solr** a Solr 9.8 a 8983-as porton. A keresési indexet a `collection1`-ben tartja.

**solr-updater**, a gazdagép 8984-es portján, az index sémáját a Dataverse metaadatmezőihez igazítja. A két szolgáltatás a Solr-adatköteten osztozik.

**smtp** tesztpostafiók (MailDev). Teszt üzemmódban a regisztrációs linkek és a jelszó-visszaállítások a http://localhost:1080 címen jelennek meg, és a konténer leállásakor eltűnnek. Éles üzemben a beállításokról bővebben itt: [Levelezés](#levelezés).

**dataverse-rocrate-preview**, a 8985-ös porton, RO-Crate-előnézetet készít, amikor a Dataverse kéri.

**previewers-provider**, a 9080-as porton, a böngészőben használt fájlelőnézeteket szolgálja ki.

**dv_initializer** a Dataverse indulása előtt előkészíti a fájltár kötetét és a nyelvi csomagok könyvtárát.

**solr_initializer** előkészíti a Solr-kötetet, és átmásolja a konfigurációját.

**bootstrap** ezután létrehozza a `dataverseAdmin` felhasználót és a gyökérgyűjteményt.

**arp-setup** Hunverse előkészítése — angol és magyar nyelv, kinézet, CEDAR, terminológiai szolgáltatás és az AROMA címe —, és API-kulcs esetén importálja a CEDAR-sablonokat.

**register-previewers** regisztrálja a fájlelőnézeteket a Dataverse-ben.

A CEDAR sémaregisztert, a terminológiai szolgáltatást és a nyelvi csomagokat a Hunverse hálozaton keresztül éri el.

## Futtatás

### Előfeltételek

A fájlok letöltése és a Hunverse indítása előtt telepítendő:

- [Docker](https://docs.docker.com/get-docker/), a Compose kiégészítővel együtt (`docker compose`). A Docker Desktop tartalmazza. Linuxon a Docker Engine és a `docker-compose-plugin` csomag telepítendő.
- [curl](https://curl.se/), az alábbi letöltőparancsok ezt használják.

A gazdagépnek kimenő internetkapcsolat szükséges ahhoz, hogy a Compose le tudja húzni az image-eket, a Hunverse pedig elérje a CEDAR-regisztert, a terminológiai szolgáltatást és a nyelvi csomagokat.

A Hunverse futtatásához nem szükséges a teljes repozitórium letöltése. A két alábbi fájl letöltése egy üres mappába elegendő.

- [hunverse-compose.yml](https://raw.githubusercontent.com/dsd-sztaki-hu/dataverse/refs/heads/hunverse-6.9/hunverse-compose.yml)
- [.env.example](https://raw.githubusercontent.com/dsd-sztaki-hu/dataverse/refs/heads/hunverse-6.9/.env.example)

```bash
mkdir hunverse && cd hunverse
curl -fsSLO https://raw.githubusercontent.com/dsd-sztaki-hu/dataverse/refs/heads/hunverse-6.9/hunverse-compose.yml
curl -fsSLO https://raw.githubusercontent.com/dsd-sztaki-hu/dataverse/refs/heads/hunverse-6.9/.env.example
```

A beállítások többségének van alapértéke, ezért a Hunverse a `.env.example` `.env` névre másolása és az `ARP_CEDAR_PROXY_API_KEY` megadása után azonnal indítható.

```bash
cp .env.example .env
```

### Docker konfigurálása éles használathoz

A Hunverse indítása előtt konfigurálni kell a Docker számára rendelkezésre álló memória- és lemezterületet. A `hunverse-compose.yml` nem korlátozza a Dataverse-konténer memóriáját. A Dockernek adott memória az egész környezet felső határa.

A Payara a Java-heap maximális méretét a számára elérhető RAM 70%-ában határozza meg (`MaxRAMPercentage=70`). Docker Desktop használatakor a rendelkezésre álló memória a Settings → Resources → Memory menüpontban állítható be. Linuxon a gazdagép fizikai memóriája az irányadó.

A `/tmp` és a `/dumps` könyvtárak memóriában tárolódnak. Ha nem adunk meg `size` értéket, a Linux mindkét csatolás számára a rendelkezésre álló RAM felét engedélyezi. A memóriahasználatot csak a ténylegesen kiírt adatok mérete határozza meg.

A fájlok feltöltése, kicsomagolása és táblázatos beolvasása során a rendszer ideiglenes másolatokat hoz létre a `/tmp` könyvtárban. Emiatt egy nagyobb fájl a feldolgozás teljes ideje alatt jelentős memóriát foglalhat. A letétbe helyezett fájlok ezzel szemben a `dv_data` köteten, lemezen tárolódnak.

#### Memória

A javasolt kiindulási érték **16 GB**. Ez elegendő a Hunverse, a Postgres és a Solr együttes futtatásához, ha a feldolgozott fájlok mérete néhány száz megabájton belül marad, és egyszerre csak néhány feltöltés zajlik.

A Java-heap mérete a Docker számára rendelkezésre álló memória 70%-áig növekedhet, így a memória fennmaradó 30%-ának szabadon kell maradnia. Ebből kell biztosítani a `/tmp` könyvtár, valamint a Postgres és a Solr memóriaigényét.

A beolvasás során egy fájlból akár három ideiglenes másolat is létrejöhet a `/tmp` alatt. Emiatt a nagyobb fájlok feldolgozása jelentős memóriaigénnyel járhat. A fennmaradó memóriából körülbelül **4 GB-ot** érdemes fenntartani a Postgres és a Solr számára.

```text
Docker-memória (GB) ≈ (3 × a legnagyobb fájl GB-ban + 4) / 0.3
```

| Legnagyobb fájl | Docker-memória |
|---|---|
| 1 GB | 24 GB |
| 2 GB | 34 GB |
| 4 GB | 54 GB |

Kisebb gépen érdemes korlátozni a heap méretét, mivel alapértelmezés szerint a Docker számára rendelkezésre álló memória 70%-át használhatja. Ehhez a `dataverse` szolgáltatásnál a `MEM_MAX_RAM_PERCENTAGE` értékét kell beállítani. **16 GB** memória esetén a `40` érték körülbelül **6 GB-ra** korlátozza a heap méretét, így nagyjából **10 GB** marad a `/tmp` és a többi szolgáltatás számára. A legnagyobb feldolgozható fájl méretét ez a fennmaradó memória korlátozza.

A `MEM_MAX_RAM_PERCENTAGE` az alapimage egyik hangolható beállítása. A [Dataverse 6.9 alapimage hangolható értékei](https://guides.dataverse.org/en/6.9/container/base-image.html#tunables) további olyan környezeti változókat sorolnak fel, amelyekkel a `dataverse` szolgáltatás JVM-je, a heap dumpok és a Payara működése hangolható.

A `/dumps` könyvtárba csak akkor kerül heap dump, ha az `ENABLE_DUMPS=1` be van állítva. A heap dump létrehozásához nagyjából a heap méretével megegyező mennyiségű szabad RAM szükséges. Emellett a `/dumps` csatolása alapértelmezés szerint a Docker számára rendelkezésre álló memória felére van korlátozva.

#### Lemez

A Docker számára rendelkezésre álló lemezterületet a tárolni kívánt repozitórium méretéhez kell igazítani, és további körülbelül **30 GB-ot** érdemes biztosítani az image-ek, a Postgres és a Solr számára. Docker Desktopon ez a Settings → Resources → Disk image size beállításnál adható meg. Linuxon annak a fájlrendszernek a szabad kapacitása a mérvadó, amely a `/var/lib/docker` könyvtárat tartalmazza.

A lemezterületet még a `dv_data` kötet megtelése előtt érdemes bővíteni. Az ideiglenes fájlok nem ezen a lemezen tárolódnak.

### A CEDAR API-kulcs megkeresése

A regisztráció a https://cedar.schema.researchdata.hu címen végezhető el. <br/>
A profil megnyitása után a megjelenő API-kulcsból a hexadecimális részt kell kimásolni, az `apiKey ` előtag nélkül. <br/>
A hexadecimális értéket az `.env` fájl `ARP_CEDAR_PROXY_API_KEY` mezőjébe kell beilleszteni.

A Hunverse API-kulcs nélkül is elindítható, azonban ebben az esetben az `arp-setup` nem szinkronizálja a CEDAR metaadatblokkokat. A metaadatblokkok szinkronizálásáig csak a felület érhető el, a Hunverse többi funkciója nem használható.

A CEDAR API-kulcs később is megadható, ezt követően pedig a metaadatblokkok importja újra futtatható. A részleteket a [CEDAR-sablonok](#cedar-sablonok) szakasz ismerteti.

### A Hunverse indítása

```bash
docker compose -f hunverse-compose.yml up
```

Az indulás több percet vesz igénybe. Az oldal a **HUNVERSE READY!** felirat megjelenése után nyitható meg.

Alapértelmezett belépési adatok:

```
http://localhost:8080
felhasználó: dataverseAdmin
jelszó: admin1
```

A Hunverse leállítása:

```bash
docker compose -f hunverse-compose.yml down # leállítás
docker compose -f hunverse-compose.yml down -v  # leállítás és az adatok törlése
```

## A hunverse-compose.yml testreszabása

A `.env.example` fájlt `.env` néven kell lemásolni. A módosítani kívánt beállításoknál el kell távolítani a sor elején található kommentjelet. Az alábbi alapértékek a `hunverse-compose.yml` fájlban megadott `${VAR:-...}` értékeknek felelnek meg. <br/>
A testreszabható változókat szolgáltatásonként az alábbiak szerint lehet beállítani.

### Élesítés előtt módosítandó beállítások

Ezek a demóértékek nyilvános vagy megosztott gazdagépen nem biztonságosak, ezért azokat az `.env` fájlban mindenképpen módosítani kell. A többi, közvetlenül a konfigurációban megadott érték a `hunverse-compose.yml` fájlban módosítható.


```bash
DATAVERSE_ADMIN_PASSWORD=admin1  # bootstrap; a beépített dataverseAdmin jelszava
DATAVERSE_DB_USER=dataverse  # Postgres-felhasználó; a dataverse is ezt használja
DATAVERSE_DB_PASSWORD=secret  # Postgres-jelszó; a dataverse is ezt használja
DATAVERSE_CORS_ORIGIN=*  # a böngésző CORS allow-origin értéke
DATAVERSE_PID_FAKE_AUTHORITY=10.5072  # hamis DOI-authority
DATAVERSE_PID_FAKE_SHOULDER=FK2/  # hamis DOI-shoulder
```

A `DATAVERSE_DB_USER` és a `DATAVERSE_DB_PASSWORD` értéke az első indítás után csak a Postgres-kötet újralétrehozásával módosítható. Ehhez a következő parancs futtatása szükséges:

`docker compose -f hunverse-compose.yml down -v`

A `hunverse-compose.yml` fájlban alapértelmezés szerint az alábbi értékek vannak beállítva:

* Közzétett portok: `5432` (Postgres), `8983` (Solr) és `8686` (JMX).
* A `DATAVERSE_SITEURL` értéke HTTP-t használ, és a `MACHINE_IP` értékén alapul (TLS nélkül).
* Az `smtp` szolgáltatás (MailDev), valamint a `25`-ös és `1080`-as port közzé van téve. További információ a [Levelezés](#levelezés) szakaszban található.
* A bootstrap nem biztonságos módban fut, így a gazdagépről indított adminisztrációs API-hívásokhoz nincs szükség unblock kulcsra.

A telepítés a Dataverse 6.9 konfigurációs útmutatójának [Securing Your Installation](https://guides.dataverse.org/en/6.9/installation/config.html#securing-your-installation) fejezete alapján tovább konfigurálható és biztonságosabbá tehető.


### CEDAR

```bash
ARP_CEDAR_PROXY_API_KEY=  # csak a hex, innen: https://cedar.schema.researchdata.hu
CEDAR_IMPORT_FOLDER_ID=https://repo.schema.researchdata.hu/folders/49ba90b3-86ee-45b8-a623-d4a7a7df926c  # nyilvános ARP-sablonmappa; csak másik mappa importálásakor módosítandó
```

### Dataverse

```bash
DATAVERSE_IMAGE=harbor.sztaki.hu/arp/hunverse:6.9
MACHINE_IP=localhost  # az oldal URL-je http://${MACHINE_IP}:8080; másik gépről történő eléréshez elérhető IP-cím szükséges
ARP_CEDAR_DOMAIN=schema.researchdata.hu  # üzemeltetett sémaregiszter
ARP_CEDAR_PROXY_API_KEY=  # lásd: A CEDAR API-kulcs megkeresése
ARP_AROMA_ADDRESS=http://localhost:8080/aroma  # a Dataverse-be csomagolt AROMA felület
TERMINOLOGY_URL_BRANCHES=https://terminology.schema.researchdata.hu/bioportal/ontologies/%s/classes/%s/descendants?page=1&pageSize=500
TERMINOLOGY_URL_VALUESETS=https://terminology.schema.researchdata.hu/bioportal/vs-collections/%s/value-sets/%s/values?page=1&pageSize=%s
TERMINOLOGY_URL_ONTOLOGIES=https://terminology.schema.researchdata.hu/bioportal/ontologies/%s/classes?page=1&pageSize=500
ARP_W3ID_BASE=https://w3id.org/arp/dev
ARP_LANG_PACKS_UPDATE_ON_START=true  # en_US / hu_HU ráhelyezése a GitHubról a /dv/langBundles könyvtárra
ARP_LANG_PACKS_REPO_URL=https://github.com/dsd-sztaki-hu/dataverse-language-packs
ARP_LANG_PACKS_REF=hunverse-v6.9
DATAVERSE_DB_USER=dataverse  # egyezzen a postgres értékével
DATAVERSE_DB_PASSWORD=secret  # egyezzen a postgres értékével
DATAVERSE_CORS_ORIGIN=*  # élesítés előtt módosítandó
DATAVERSE_PID_FAKE_AUTHORITY=10.5072
DATAVERSE_PID_FAKE_SHOULDER=FK2/
```

### AROMA kliensbeállítások

Az AROMA-csomag a kliensértékeket egy objektumban, a `globalThis.__AROMA_CONFIG__`-ban tárolja. Amikor a böngésző betölt egy `/aroma` alatti JavaScript-fájlt, a Dataverse a futó konfigurációból cseréli ki ebben az objektumban az alábbi tulajdonságokat:

| AROMA-tulajdonság | Forrása |
|---|---|
| `VITE_REACT_APP_DV_HOST` | `DATAVERSE_SITEURL` (`http://${MACHINE_IP}:8080`), a záró perjel nélkül |
| `VITE_REACT_APP_CEDAR_DOMAIN` | `arp.cedar.domain` (`ARP_CEDAR_DOMAIN`) |
| `VITE_REACT_APP_W3ID_BASE` | `arp.w3id.base` (`ARP_W3ID_BASE`), a záró perjel nélkül |
| `VITE_REACT_APP_CEDAR_PROXY` | `{DATAVERSE_SITEURL}/api/arp/cedarResourceProxy/` |

Az `arp-setup` az `arp.cedar.domain` és az `arp.w3id.base` értékét az adatbázisban tárolja. Az adatbázisban szereplő értékek elsőbbséget élveznek a környezeti változókkal szemben. Az `ARP_CEDAR_DOMAIN` vagy az `ARP_W3ID_BASE` módosítása után az alábbi parancsot kell futtatni:

```bash
docker compose -f hunverse-compose.yml up arp-setup
```

Ezt követően az AROMA-t újra kell tölteni. Ehhez elegendő az AROMA újratöltése, a Dataverse-konténer újralétrehozására nincs szükség.

A `DATAVERSE_SITEURL` nem adatbázisban tárolt beállítás, hanem az `.env` fájlban megadott `MACHINE_IP` értékéből származik. A `MACHINE_IP` módosítása után a Dataverse-konténert újra létre kell hozni.


```bash
docker compose -f hunverse-compose.yml up -d dataverse
```

A gazdagép címe és a CEDAR-proxy is az új oldal URL-jét követi.

### Kinézet

A Hunverse megjelenése már része a Dataverse-image-nek, ezért a Hunverse indításához nincs szükség helyi arculatmappára.

Ha a bannereket, a CSS-t vagy a láblécpecséteket módosítani szeretnénk, az image-ben található arculati fájlokat egy helyi mappával lehet felülírni:

1. A jelenlegi témát a futó konténerből kell kimásolni:

```bash
docker cp dataverse:/opt/payara/deployments/dataverse/branding ./branding
```

2. A `./branding` mappában található fájlok szerkeszthetők. A fájlneveket változatlanul kell hagyni, vagy az útvonalakat frissíteni kell a `branding-config.js` fájlban.

3. Az `.env` fájlban meg kell adni az arculatmappa elérési útját:

```bash
BRANDING_DIR=./branding
```

4. A `hunverse-compose.yml` fájlban az arculathoz tartozó két kötéssort ki kell venni a megjegyzésből:

```yaml
- ${BRANDING_DIR}:/var/www/dataverse/branding:ro
- ${BRANDING_DIR}:/opt/payara/deployments/dataverse/branding:ro
```

5. A Dataverse-konténert újra kell létrehozni, majd a böngészőt frissíteni kell:

```bash
docker compose -f hunverse-compose.yml up -d dataverse
```

A módosított CSS- és PNG-fájlok a frissítést követően érvénybe lépnek. Ha a navigációs sáv logófájljának neve megváltozik, a `LOGO_CUSTOMIZATION_FILE` értékét is módosítani kell (például `/branding/mylogo.png` értékre), majd futtatni kell az alábbi parancsot:

```bash
docker compose -f hunverse-compose.yml up arp-setup
```


| Fájl | Szerep |
|---|---|
| `custom-stylesheet.css` | Színek és elrendezés |
| `custom-header.html` | Felső banner jelölése |
| `custom-footer.html` | Lábléc: szerzői jog és pecsétek |
| `branding-config.js` | `topbanner`, `navbarLogo`, `footerstamps` |
| `topbanner_hunverse_002_eng.png` | Angol fejlécbanner |
| `topbanner_hunverse_002.png` | Magyar fejlécbanner |
| `topbanner_hunverse_002_dark425_eng.png` | Angol navigációs sáv logó |
| `topbanner_hunverse_002_dark425.png` | Magyar navigációs sáv logó |
| `hunverse_portal2_btn.png` | Magyar portálgomb |
| `hunverse_portal_button_en_001.png` | Angol portálgomb |
| `unified_search_btn.png` | Magyar föderált kereső gomb |
| `federated_search_button_en_003.png` | Angol föderált kereső gomb |

A `footerstamps` a `branding-config.js`-ben alapértelmezés szerint üres lista. Pecsét megjelenítéséhez `{ src, href, alt, width }` objektumok adandók hozzá.

### arp-setup

```bash
ARP_CEDAR_DOMAIN=schema.researchdata.hu
ARP_CEDAR_PROXY_API_KEY=  # lásd: A CEDAR API-kulcs megkeresése; ha üres, kihagyja az importot és az arp.cedar.proxyApiKey-t
ARP_AROMA_ADDRESS=http://localhost:8080/aroma
ARP_W3ID_BASE=https://w3id.org/arp/dev
CEDAR_IMPORT_DV=root  # a gyűjtemény, amely az importált CEDAR-sablonokat kapja
CEDAR_IMPORT_FOLDER_ID=https://repo.schema.researchdata.hu/folders/49ba90b3-86ee-45b8-a623-d4a7a7df926c
LOGO_CUSTOMIZATION_FILE=/branding/topbanner_hunverse_002_dark425.png  # a navigációs sáv logójának útvonala a konténerben
TERMINOLOGY_URL_TEMPLATE=https://terminology.schema.researchdata.hu/bioportal/ontologies/%s/classes/%s/descendants?pageSize=500  # csak a setup használja
TERMINOLOGY_URL_BRANCHES=https://terminology.schema.researchdata.hu/bioportal/ontologies/%s/classes/%s/descendants?page=1&pageSize=500
TERMINOLOGY_URL_VALUESETS=https://terminology.schema.researchdata.hu/bioportal/vs-collections/%s/value-sets/%s/values?page=1&pageSize=%s
TERMINOLOGY_URL_ONTOLOGIES=https://terminology.schema.researchdata.hu/bioportal/ontologies/%s/classes?page=1&pageSize=500
```

### previewers-provider és register-previewers

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
DATAVERSE_DB_USER=dataverse  # POSTGRES_USER; egyezzen a dataverse értékével
DATAVERSE_DB_PASSWORD=secret  # POSTGRES_PASSWORD; egyezzen a dataverse értékével
```

Csak a `postgres_data` kötet első létrehozásakor érvényesül.

### Levelezés

A Hunverse e-mailt küld többek között regisztrációkor, jelszó-visszaállításkor, valamint a kapcsolatfelvételi űrlap használatakor.

Alapértelmezés szerint a Hunverse a MailDev tesztpostafiókot használja. A levelezőszerver neve `smtp`, a port `25`, hitelesítésre nincs szükség, a feladó pedig `dataverse@localhost`. A regisztrációs és jelszó-visszaállítási levelek a http://localhost:1080 címen tekinthetők meg. A Hunverse leállításakor a tesztpostafiók tartalma törlődik.

Éles használathoz az `.env` fájlban kell megadni a levelezőszerver adatait.

Az alábbi értékeket az `.env` fájlba kell beírni, a példák helyére a használt postafiók adatait megadva. A szóközt tartalmazó értékeket idézőjelbe kell tenni. Ha a jelszó `$` karaktert tartalmaz, azt `$$` formában kell megadni.

```bash
# A feladó címe. A postafiók saját címét kell megadni.
DATAVERSE_MAIL_SYSTEM_EMAIL="Hunverse <noreply@example.org>"

# A Támogatás űrlapon küldött üzenetek erre a címre érkeznek. Nem kötelező.
# Ha nincs megadva, a támogatási üzenetek a fenti feladó címére érkeznek.
DATAVERSE_MAIL_SUPPORT_EMAIL=support@example.org

# A kimenő levelezőszerver neve és portja.
DATAVERSE_MAIL_MTA_HOST=smtp.example.org
DATAVERSE_MAIL_MTA_PORT=587

# Felhasználónév és jelszó.
# A felhasználónév gyakran megegyezik a postafiók címével.
DATAVERSE_MAIL_MTA_AUTH=true
DATAVERSE_MAIL_MTA_USER=noreply@example.org
DATAVERSE_MAIL_MTA_PASSWORD=mailpassword

# Az 587-es port használatához szükséges.
DATAVERSE_MAIL_MTA_STARTTLS_ENABLE=true

# A 465-ös port használatához ezt a beállítást kell használni.
# A fenti STARTTLS sort ebben az esetben ki kell kommentelni.
# DATAVERSE_MAIL_MTA_SSL_ENABLE=true
```

Az éles levelezési beállítások megadása után a tesztpostafiókot ki kell kapcsolni. Ehhez a `hunverse-compose.yml` fájlban ki kell kommentezni az `smtp` szolgáltatást.

A Hunverse a levelezési beállításokat a konténer újralétrehozásáig használja. Az új beállítások az alábbi paranccsal léptethetők életbe:

```bash
docker compose -f hunverse-compose.yml up -d --remove-orphans
```

A tesztpostafiók konténere a szolgáltatás kikommentelése után ezzel a paranccsal eltávolításra kerül.

A levelezés működése egy olyan felhasználói fiók jelszavának visszaállításával ellenőrizhető, amelyhez hozzáférhető postafiók tartozik. A jelszó-visszaállítási üzenetnek erre a címre kell megérkeznie.

Ha a levél nem érkezik meg, az `.env` fájlban engedélyezhető a levelezés hibakeresése:

```bash
DATAVERSE_MAIL_DEBUG=true
```

Ezután újra kell futtatni a fenti `docker compose` parancsot, majd ellenőrizni kell a Dataverse naplóját.

További levelezési beállítások a [SMTP/Email Configuration](https://guides.dataverse.org/en/6.9/installation/config.html#smtp-email-configuration) fejezetben találhatók.


## arp-setup

A `docker compose -f hunverse-compose.yml up` akkor futtatja, amikor a Dataverse és a `bootstrap` készen áll.

Megvárja az `/api/info/version` választ és a gyökér dataverse-t, majd PUT-tal beállítja:

- `:FilePIDsEnabled`, `:AllowEnablingFilePIDsPerCollection`
- `arp.w3id.base`, `arp.cedar.domain`, `arp.aroma.address`, `arp.cedar.proxyApiKey`
- `terminology.url.template`, `terminology.url.branches`, `terminology.url.valueSets`, `terminology.url.ontologies`
- `:HeaderCustomizationFile`, `:FooterCustomizationFile`, `:StyleCustomizationFile`, `:LogoCustomizationFile`
- `:Languages`, `:MetadataLanguages` (angol + magyar)

Ha az `ARP_CEDAR_PROXY_API_KEY` és a `CEDAR_IMPORT_FOLDER_ID` is meg van adva, a sablonokat is importálja CEDAR-ból az `importTemplatesFromCedarFolder` hívással. Ha bármelyik üres, a beállítások akkor is érvényesülnek, de az importálás kimarad.

Az `arp-setup` újrafuttatandó, ha a fenti `.env` értékek módosultak, ha az első import nem sikerült, vagy ha a navigációs sáv logójának fájlneve változott:

```bash
docker compose -f hunverse-compose.yml up arp-setup
```

## Beállítási végpontok

A bootstrap nem biztonságos módban fut. A gazdagépről indított curl-hez nem kell unblock kulcs.

### Beállítások

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

### Fordítások

Az `en_US` és `hu_HU` fordítási fájlokat a GitHubról a `/dv/langBundles` könyvtárban tárolja. A CEDAR UUID-khez tartozó tulajdonságfájlokat nem törli.

Ha nincs megadva JSON-törzs, a compose konfigurációjában és az `ArpConfig` beállításaiban megadott repo- és ref-értékeket használja.


```bash
curl -X POST 'http://localhost:8080/api/admin/langPacks/update' \
  -H 'Content-Type: application/json' \
  -d '{"repoUrl":"https://github.com/dsd-sztaki-hu/dataverse-language-packs","ref":"hunverse-v6.9"}'
```

### CEDAR-sablonok

Ez akkor használandó, ha az első induláskor az `ARP_CEDAR_PROXY_API_KEY` nem került megadásra. A kulcs elmentése után ugyanaz az import futtatandó, amelyet az `arp-setup` futtatott volna.

```bash
curl -X PUT -d '<hex>' \
  http://localhost:8080/api/admin/settings/arp.cedar.proxyApiKey
```

```bash
curl -X POST 'http://localhost:8080/api/admin/arp/importTemplatesFromCedarFolder' \
  -H 'Content-Type: application/json' \
  -d '{"folderId":"https://repo.schema.researchdata.hu/folders/49ba90b3-86ee-45b8-a623-d4a7a7df926c","dvIdtf":"root"}'
```

## Kötetek

Elnevezett kötetek (a `docker compose -f hunverse-compose.yml down -v` parancsig megmaradnak):

| Kötet | Csatolás | Tartalom |
|---|---|---|
| `dv_data` | `/dv` | feltöltött fájlok, nyelvi csomagok, exportőrök |
| `dv_secrets` | `/secrets` | titkok |
| `postgres_data` | `/var/lib/postgresql/data` | adatbázis |
| `solr_data` | `/var/solr` | Solr-index |
| `solr_conf` | Solr-sablon | a `solr_initializer` által másolt Solr-konfiguráció |

Opcionális bind mountok (a `hunverse-compose.yml`-ben kikommentezve; egyedi megjelenéshez lásd az [Arculat](#arculat) szakaszt):

```
${BRANDING_DIR} → /var/www/dataverse/branding
${BRANDING_DIR} → /opt/payara/deployments/dataverse/branding
```

tmpfs (a konténer leállításakor törlődik): a Dataverse `/dumps` és `/tmp` könyvtára, valamint az smtp `/mail` könyvtára. A `/dumps` és a `/tmp` mérete a `hunverse-compose.yml`-ben nincs megadva. Lásd a [Docker-korlátok éles használathoz](#docker-korlátok-éles-használathoz) szakaszt.

## Portok

| Port | Mi |
|---|---|
| 8080 | Dataverse |
| 1080 | Tesztpostafiók a helyi levélhez. Lásd a [Levelezés](#levelezés) szakaszt. |
| 8983 | Solr |
| 5432 | Postgres |
| 8984 | Solr-frissítő |
| 8985 | RO-Crate-előnézet |
| 9080 | Fájlelőnézetek |

## Image-ek

| Szolgáltatás | Image |
|---|---|
| `dataverse` | `harbor.sztaki.hu/arp/hunverse:6.9` |
| `bootstrap` | `gdcc/configbaker:6.9-noble-r6` |
| `arp-setup` | `gdcc/configbaker:6.9-noble-r6` |
| `dv_initializer` | `gdcc/configbaker:6.9-noble-r6` |
| `solr_initializer` | `gdcc/configbaker:6.9-noble-r6` |
| `postgres` | `postgres:16` |
| `solr` | `solr:9.8.0` |
| `smtp` | `maildev/maildev:2.0.5` |
| `solr-updater` | `harbor.sztaki.hu/arp/dataverse-solr-updater:6.9` |
| `dataverse-rocrate-preview` | `harbor.sztaki.hu/arp/dataverse-rocrate-preview:6.9` |
| `previewers-provider` | `trivadis/dataverse-previewers-provider:latest` |
| `register-previewers` | `trivadis/dataverse-deploy-previewers:latest` |
