#!/bin/bash
#
# cpwebapp <project dir> <file in webapp>
#
# Use it either as an external tool and trigger via menu or shortcut, or add a File watcher by loading
# watchers.xml into an IntelliJ IDE, and let it do the copying whenever you say a file under webapp.
#

PROJECT_DIR=$1
FILE_TO_COPY=$2
RELATIVE_PATH="${FILE_TO_COPY#$PROJECT_DIR/}"

#echo "project dir: |$$PROJECT_DIR|"
#echo "File to copy: |$FILE_TO_COPY|"
#echo "RELATIVE_PATH: |$RELATIVE_PATH|"

# Check if RELATIVE_PATH starts with 'src/main/webapp', otherwise ignore
if [[ $RELATIVE_PATH == src/main/webapp* ]]; then
    # get current version
    V=`perl -ne 'print $1 if /<revision>(.*?)<\/revision>/' ./modules/dataverse-parent/pom.xml`
    #echo "Version: $V"

    RELATIVE_PATH_WITHOUT_WEBAPP="${RELATIVE_PATH#src/main/webapp/}"

    TARGET_DIR=./docker-dev-volumes/glassfish/applications/dataverse-$V
    TARGET_PATH="${TARGET_DIR}/${RELATIVE_PATH_WITHOUT_WEBAPP}"

    mkdir -p "$(dirname "$TARGET_PATH")"
    cp "$FILE_TO_COPY" "$TARGET_PATH"

    echo "File $FILE_TO_COPY copied to $TARGET_PATH"
fi
