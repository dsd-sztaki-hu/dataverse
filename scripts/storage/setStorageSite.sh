#!/bin/bash

status=${1}
dsversion=${2}
site=${3}

echo "INSERT INTO dvobjectremotestoragelocation VALUES ('$status' , $dsversion, $site) ON CONFLICT(datasetversion_id, site_id) DO UPDATE set status='$status'" | psql dvndb postgres

exit 0

curl -H "Content-type:application/json" \
     -H"X-Dataverse-key:$DATAVERSE_KEY" \
     -X POST \
     -d '{"datasetVersion": 1, 
          "status": "CPRQ", 
          "storageSite": {
              "id": 1, 
              "name": "", 
              "hostname": "", 
              "primaryStorage": false, 
              "transferProtocols":""
          }
         }' http://localhost:8080/api/datasets/3/versions/1/storageSite
