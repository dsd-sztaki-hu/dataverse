#!/bin/sh

SCRIPTDIR=`dirname $0`
[ -f $SCRIPTDIR/dataverse_script_config ] && . $SCRIPTDIR/dataverse_script_config

GLASSFISH_DIR=${GLASSFISH_DIR:-/usr/local/glassfish5}
ASADMIN=$GLASSFISH_DIR/bin/asadmin

# This script lists storages in the current dataverse installation.
# It also displays available disk space for each of them.
$ASADMIN list-jvm-options | grep 'files\..*\.directory=' | sed 's/.*\.\([^.]*\)\.directory=\(.*\)/\1 \2/' |
while read LABEL DIR
do
	FREE=`df -m $DIR --output=avail| tail -n1`
	PERC=`df $DIR --output=pcent|tail -n1`
	echo $LABEL $DIR $FREE $PERC
done

