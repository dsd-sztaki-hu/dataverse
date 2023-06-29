#!/bin/sh

SCRIPTDIR=`dirname $0`
[ -f $SCRIPTDIR/dataverse_script_config ] && . $SCRIPTDIR/dataverse_script_config

export GLASSFISH_DIR=${GLASSFISH_DIR:-/usr/local/payara5}
export ASADMIN=$GLASSFISH_DIR/bin/asadmin

# This script lists storages in the current dataverse installation.
# It also displays available disk space for each of them.
#$ASADMIN list-jvm-options | grep 'files\..*\.directory=' | sed 's/.*\.\([^.]*\)\.directory=\(.*\)/\1 \2/' |
echo LABEL TYPE DIR FREE FREE%
$ASADMIN list-jvm-options | grep 'files\..*\.type=' | sed 's/.*\.\([^.]*\)\.type=\(.*\)/\1 \2/' |
while read LABEL TYPE
do
	DIR=`$ASADMIN list-jvm-options | grep "files\.$LABEL\.directory=" | sed 's/.*\.directory=\(.*\)/\1/'`
	if [ "$TYPE" = 'file' ]
	then 
		FREE=`df -m $DIR --output=avail| tail -n1`
		PERC=`df $DIR --output=pcent|tail -n1`
	else
		FREE="n/a"
		PERC="n/a"
	fi
	echo $LABEL $TYPE $DIR $FREE $PERC
done

