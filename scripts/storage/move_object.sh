#!/bin/sh -e

SCRIPTDIR=`dirname $0`
[ -f $SCRIPTDIR/dataverse_script_config ] && . $SCRIPTDIR/dataverse_script_config

GLASSFISH_DIR=${GLASSFISH_DIR:-/usr/local/glassfish5}
ASADMIN=$GLASSFISH_DIR/bin/asadmin

DVNDB=${DVNDB:-dvndb}
export $DVNDB

_runsql() {
	su - postgres -c "psql $DVNDB -c \"$1\""
}

_usage(){
	echo "This script moves files/dataverses/datasets between storages."
	echo "Usage:"
	echo "$0 dataverse <fromstoragename> <tostoragename> <dataversename/id>	move dataverse and all contained datasets and files to the specified storage"
	echo "$0 dataset <fromstoragename> <storagename> <datasetname/id>	move dataset and all contained files to the specified storage"
	echo "$0 files <fromstoragename> <storagename> <filename/id> [dataset storageidentifier path]	move files to specified storage"
	exit 0
}

if [ -z "$4" ]
then
	_usage
fi

FROM=$2
TO=$3
OBJ=$4

if [ "$1" = "dataverse" ]
then
	if !(echo "$OBJ" | grep '^[0-9]\+$')
	then
		ID=`_runsql "SELECT id FROM dvobject NATURAL JOIN dataverse WHERE alias='$OBJ'" | grep '^ *[0-9]\+ *$'`
	else
		ID=$OBJ
	fi
	echo "setting storageidentifier for dataverse $OBJ (id $ID) from $FROM to $TO"
	_runsql "UPDATE dvobject SET storageidentifier=REPLACE(storageidentifier,'$FROM://','$TO://') WHERE id=$ID"
	echo "setting storageidentifier for all sub-dataverses"
	_runsql "SELECT id FROM dvobject NATURAL JOIN dataverse WHERE owner_id=$ID" | grep '^ *[0-9]\+ *$' |
	while read subid
	do
		$0 dataverse $FROM $TO $subid
	done
	echo "setting storageidentifier for all datasets"
	_runsql "SELECT id FROM dvobject NATURAL JOIN dataset WHERE owner_id=$ID" | grep '^ *[0-9]\+ *$' |
	while read subid
	do
		$0 dataset $FROM $TO $subid
	done
	# AFAIK, a dataverse cannot contain files, so not moving them directly
elif [ "$1" = "dataset" ]
then
	if !(echo "$OBJ" | grep '^[0-9]\+$')
	then
		ID=`_runsql "SELECT ds1.id FROM dataset ds1 NATURAL JOIN dvobject dvo1 JOIN (datafile df2 NATURAL JOIN dvobject dvo2) ON ds1.id=dvo2.owner_id
		             WHERE ds1.id IN (SELECT DISTINCT owner_id FROM dvobject WHERE storageidentifier LIKE '$2://%') GROUP BY ds1.id,dvo1.identifier"`
	else
		ID=$OBJ
	fi
	echo "getting storageidentifier path, frompath and topath for dataset"
	datasetpath=`_runsql "SELECT REPLACE(REPLACE(storageidentifier,'$FROM://',''),'$TO://','') FROM dvobject WHERE id=$ID" | head -n3 | tail -n1 | sed 's/ *//g'`
	frompath=`$ASADMIN list-jvm-options | grep 'files\..*\.directory=' | sed 's/.*\.\([^.]*\)\.directory=\(.*\)/\1 \2/' | grep "^$FROM " | cut -d' ' -f2`
	topath=`$ASADMIN list-jvm-options | grep 'files\..*\.directory=' | sed 's/.*\.\([^.]*\)\.directory=\(.*\)/\1 \2/' | grep "^$TO " | cut -d' ' -f2`
	
	echo "setting storageidentifier for dataset $OBJ (id $ID) from $FROM to $TO"
	_runsql "UPDATE dvobject SET storageidentifier=REPLACE(storageidentifier,'$FROM://','$TO://') WHERE id=$ID"
	
	echo "creating storageidentifier path at $TO if necessary"
	mkdir -p $topath/$datasetpath
	chown payara.payara $topath/$datasetpath
	
	echo "setting storageidentifier for all files in the dataset"
	_runsql "SELECT id FROM dvobject NATURAL JOIN datafile WHERE owner_id=$ID" | grep '^ *[0-9]\+ *$' |
	while read subid
	do
		$0 file $FROM $TO $subid $datasetpath $frompath $topath || true
	done
elif [ "$1" = "file" ]
then
	ID=$OBJ
	if [ -z "$5" ]
	then
		echo "getting storageidentifier path for dataset"
		datasetpath=`_runsql "SELECT REPLACE(REPLACE(ds.storageidentifier,'$FROM://',''),'$TO://','') FROM dvobject ds, dvobject f WHERE f.id=$ID AND ds.id=f.owner_id " | head -n3 | tail -n1 | sed 's/^ *//;s/ *$//'`
	else
		datasetpath=$5
	fi
	if [ -z "$6" ]
	then
		echo "getting frompath"
		frompath=`$ASADMIN list-jvm-options | grep 'files\..*\.directory=' | sed 's/.*\.\([^.]*\)\.directory=\(.*\)/\1 \2/' | grep "^$FROM " | cut -d' ' -f2`
	else
		frompath=$6
	fi
	if [ -z "$7" ]
	then
		echo "getting topath"
		topath=`$ASADMIN list-jvm-options | grep 'files\..*\.directory=' | sed 's/.*\.\([^.]*\)\.directory=\(.*\)/\1 \2/' | grep "^$TO " | cut -d' ' -f2`
	else
		topath=$7
	fi
	
	echo "getting file name"
	filename=`_runsql "SELECT REPLACE(REPLACE(storageidentifier,'$FROM://',''),'$TO://','') FROM datafile NATURAL JOIN dvobject WHERE id=$ID" | head -n3 | tail -n1 | sed 's/^ *//;s/ *$//'`
	
	echo "copying file $OBJ (id $ID) FROM $FROM (path $frompath) to $TO (path $topath), datasetpath $datasetpath"
	#cp $frompath/$datasetpath/$filename $topath/$datasetpath/$filename
	rsync --inplace $frompath/$datasetpath/$filename $topath/$datasetpath/$filename
	#chown payara.payara $topath/$datasetpath/$filename
	
	echo "setting storageidentifier for file $OBJ (id $ID) from $FROM to $TO"
	_runsql "UPDATE dvobject SET storageidentifier=REPLACE(storageidentifier,'$FROM://','$TO://') WHERE id=$ID"
	
	echo "removing file $OBJ (id $ID) FROM $FROM (path $frompath), datasetpath $datasetpath"
	rm $frompath/$datasetpath/$filename
else
	_usage
fi
