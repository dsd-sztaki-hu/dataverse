# Initial setup

1. Clone and build ARP specific services
    ```
    git clone git@git.sztaki.hu:dsd/dataverse-solr-updater.git
    DOCKER_BUILDKIT=1 docker build -t dataverse-solr-updater:latest ./dataverse-solr-updater
    
    git clone git@git.sztaki.hu:dsd/dataverse-rocrate-preview.git
    DOCKER_BUILDKIT=1 docker build -t dataverse-rocrate-preview:latest ./dataverse-rocrate-preview
    ```
1. Build Dataverse base image (need to be run only once for a specific Dataverse version)
    ```   
    mvn -Pct -f modules/container-base install
    ```
1. Build Dataverse image (do this every time you change sources and want to rebuild image)
    ```
    mvn -Pct clean package
    ```
   
# Running the stack

```
mvn -Pct docker:run
```

http://localhost:8080
```
User: dataverseAdmin 
Pass: admin1
```

Stopping the stack (ometimes have to do explicitly because dev_smtp is not killed by Ctrl+C):

```
mvn -Pct docker:stop
```

> Note: you cannot run [docker-compose-dev.yml](docker-compose-dev.yml) directly, eg. `docker-compose -f docker-compose-dev.yml up` won't work. Also `docker:run` doesn't allow running more than one docker-compose.yaml files. This is a pitty, otherwise we could have our own separate `docker-compose-arp.yml` with our overrides and settings and could leave the original [docker-compose-dev.yml](docker-compose-dev.yml) intact. For now, we had to do our changes and additions right in [docker-compose-dev.yml](docker-compose-dev.yml). 

# Allow connection from anywhere

Enable CORS with "Access-Control-Allow-Origin: "*":

```
curl -X PUT -d 'true' http://localhost:8080/api/admin/settings/:AllowCors
```

# Enable file pid

Be default the file PIDs are not enabled. It has to be enabled globally (:FilePIDsEnabled) and then also AllowEnablingFilePIDsPerCollection to that we can set it on the Root dataverse as well:

```
curl -X PUT -d 'true' http://localhost:8080/api/admin/settings/:FilePIDsEnabled
```

Maybe also these, but not sure:
```
curl -X PUT -d 'true' http://localhost:8080/api/admin/settings/:AllowEnablingFilePIDsPerCollection
curl -X PUT -H "X-Dataverse-key:$API_TOKEN" "http://localhost:8080/api/dataverses/root/attribute/filePIDsEnabled?value=true"
```


# Other development settings

Builtin users for unit tests:

```
curl -X PUT -d 'burrito' http://localhost:8080/api/admin/settings/BuiltinUsers.KEY
```

# Sync CEDAR and DV metaadatablocks

Sync CEDAR and DV metaadatablocks. Note that we use `arp.orgx` base domain. We can do this, because of the `extra_hosts` defines in [docker-compose-dev.yml](docker-compose-dev.yml)

```
curl -X POST 'http://localhost:8080/api/admin/arp/syncMdbsWithCedar' \
-H 'Content-Type: application/json' \
-d '{
  "mdbParams": [
    {"name": "citation"},
    {"name": "geospatial", "namespaceUri": "https://dataverse.org/schema/geospatial/"},
    {"name": "socialscience", "namespaceUri": "https://dataverse.org/schema/socialscience/"},
    {"name": "biomedical", "namespaceUri": "https://dataverse.org/schema/biomedical/"},
    {"name": "astrophysics", "namespaceUri": "https://dataverse.org/schema/astrophysics/"},
    {
      "name": "journal",
      "namespaceUri": "https://dataverse.org/schema/journal/",
      "cedarUuid": "aaaaaaaa-bbbb-cccc-dddd-65d43571f306"
    }
  ],
  "cedarParams": {
    "cedarDomain": "arp.orgx",
    "apiKey": "0000111122223333444455556666777788889999aaaabbbbccccddddeeeeffff",
    "folderId": "https:%2F%2Frepo.arp.orgx%2Ffolders%2F8b8c279f-54fd-4b56-87c0-3954a4c1f0dd"
  }
}'
```


# Dev setup

```
curl -X POST -H "Content-Type: application/json" -H "Authorization: Basic $(echo -n admin:admin | base64)" http://localhost:4849/management/domain/domain1/applications/application/dataverse/_undeploy
```
- https://dataverse-guide--9959.org.readthedocs.build/en/9959/container/dev-usage.html#ide-triggered-re-deployments
- Import [watchers.xml](scripts%2Fintellij%2Fwatchers.xml) file watcher to have xhtml, js, etc files automatically updated in the container
    - See discussion here: https://dataverse.zulipchat.com/#narrow/stream/375812-containers/topic/faster.20redeploy/near/415973553
