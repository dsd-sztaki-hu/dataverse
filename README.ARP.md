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

# Dev setup


- https://dataverse-guide--9959.org.readthedocs.build/en/9959/container/dev-usage.html#ide-triggered-re-deployments
- Import [watchers.xml](scripts%2Fintellij%2Fwatchers.xml) file watcher to have xhtml, js, etc files automatically updated in the container
    - See discussion here: https://dataverse.zulipchat.com/#narrow/stream/375812-containers/topic/faster.20redeploy/near/415973553
