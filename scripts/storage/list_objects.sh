#!/bin/sh

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
	echo "This script lists files/dataverses/datasets in a dataverse storage and/or dataverse or dataset."
	echo "Usage:"
	echo "$0 dataverses <storagename>	list dataverses that have datasets on a given storage"
	echo "$0 datasets <storagename> [dataverse]	list datasets that have files on a given storage and optionally dataverse"
	echo "$0 files <storagename> [dataverse <dataversename>]	list files on a given storage and optionally dataverse"
	echo "$0 files <storagename> [dataset <satasetname>]	list files on a given storage and optionally dataset"
	exit 0
}

if [ -z "$2" ]
then
	_usage
fi

if [ "$1" = "dataverses" ]
then
	_runsql "SELECT id, alias, description FROM dataverse WHERE id IN (SELECT DISTINCT owner_id FROM dataset NATURAL JOIN dvobject WHERE storageidentifier LIKE '$2://%')"
elif [ "$1" = "datasets" ]
then
	if [ -z "$3" ]
	then
		_runsql "SELECT ds1.id, dvo1.identifier, sum(filesize) FROM dataset ds1 NATURAL JOIN dvobject dvo1 JOIN (datafile df2 NATURAL JOIN dvobject dvo2) ON ds1.id=dvo2.owner_id 
		         WHERE ds1.id IN (SELECT DISTINCT owner_id FROM dvobject WHERE storageidentifier LIKE '$2://%') GROUP BY ds1.id,dvo1.identifier"
	elif echo "$3" | grep '^[0-9]\+$'
	then
		_runsql "SELECT id, identifier FROM dataset NATURAL JOIN dvobject WHERE id IN (SELECT DISTINCT owner_id FROM dvobject WHERE storageidentifier LIKE '$2://%') AND owner_id=$3"
	else
		_runsql "SELECT id, identifier FROM dataset NATURAL JOIN dvobject WHERE id IN (SELECT DISTINCT owner_id FROM dvobject WHERE storageidentifier LIKE '$2://%') AND owner_id IN (SELECT id FROM dataverse WHERE alias='$3')"
	fi
elif [ "$1" = "files" ]
then
	if [ -z "$3" ]
	then
		_runsql "SELECT id, directorylabel, label, filesize, owner_id FROM datafile NATURAL JOIN dvobject NATURAL JOIN filemetadata WHERE storageidentifier LIKE '$2://%' ORDER BY owner_id ;"
	elif [ "$3" = "dataverse" ]
	then
		if echo "$4" | grep '^[0-9]\+$'
		then
			_runsql "SELECT id, directorylabel, label, filesize FROM datafile NATURAL JOIN dvobject NATURAL JOIN filemetadata WHERE storageidentifier LIKE '$2://%' AND owner_id=$4"
		else
			_runsql "SELECT id, directorylabel, label, filesize FROM datafile NATURAL JOIN dvobject NATURAL JOIN filemetadata WHERE storageidentifier LIKE '$2://%' AND owner_id IN (SELECT id FROM dataverse WHERE alias='$4')"
		fi
	elif [ "$3" = "dataset" ]
	then
		if echo "$4" | grep '^[0-9]\+$'
		then
			_runsql "SELECT id, directorylabel, label, filesize FROM datafile NATURAL JOIN dvobject NATURAL JOIN filemetadata WHERE storageidentifier LIKE '$2://%' AND owner_id=$4"
		else
			_runsql "SELECT id, directorylabel, label, filesize FROM datafile NATURAL JOIN dvobject NATURAL JOIN filemetadata WHERE storageidentifier LIKE '$2://%' AND owner_id IN (SELECT id FROM dataset WHERE alias='$4')"
		fi
	fi
else
	_usage
fi

