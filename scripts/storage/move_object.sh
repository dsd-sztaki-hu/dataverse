#!/bin/sh -e

SCRIPTDIR=`dirname $0`
[ -f $SCRIPTDIR/dataverse_script_config ] && . $SCRIPTDIR/dataverse_script_config

GLASSFISH_DIR=${GLASSFISH_DIR:-/usr/local/payara5}
ASADMIN=$GLASSFISH_DIR/bin/asadmin
DEBUG=${DEBUG:-false}

DVNDB=${DVNDB:-dvndb}
export DVNDB

TMPPATH=${TMPPATH:-/tmp/dataverse_move_object}
export TMPPATH
mkdir -p $TMPPATH

_usage(){
	echo "This script moves files/dataverses/datasets between storages."
	echo "Usage:"
	echo "    $0 dataverse <fromstoragename> <tostoragename> <dataversename/id>                          move dataverse and all contained datasets and files to the specified storage"
	echo "    $0 dataset <fromstoragename> <storagename> <datasetname/id>                                move dataset and all contained files to the specified storage"
	echo "    $0 files <fromstoragename> <storagename> <filename/id> [dataset storageidentifier path]    move files to specified storage"
	echo "VARIABLES:"
	echo "    DEBUG: $DEBUG"
	echo "    DVNDB: $DVNDB"
	echo "    TMPPATH: $TMPPATH"
	echo "    AWS_ENDPOINT_URL: $AWS_ENDPOINT_URL"
	echo "    AWS_ENDPOINT_URL2: $AWS_ENDPOINT_URL2"
	echo "    AWS_PROFILE: $AWS_PROFILE"
	exit 0
}

if [ -z "$4" ]
then
	_usage
fi

FROM=$2
TO=$3
OBJ=$4

if ! ( [ "$FROMTYPE" ] || [ "$TOTYPE" ] ) # If we were recursively called, then FROM and TO types should not be recalculated
then
	echo "Detecting FROM ($FROM) and TO ($TO) types"
	$SCRIPTDIR/list_storages.sh > /tmp/storage_list.lst
	export FROMTYPE=`grep "^$FROM" /tmp/storage_list.lst | cut -f2 -d' '`
	export TOTYPE=`grep "^$TO" /tmp/storage_list.lst | cut -f2 -d' '`
	case "$FROMTYPE" in
		s3|file)
			$DEBUG && echo "FROMTYPE: $FROMTYPE"
		s3)
			if ! ([ "$AWS_ENDPOINT_URL" ] && [ "$AWS_PROFILE" ])
			then
				echo "AWS_ENDPOINT_URL and AWS_PROFILE must be set!"
				exit 1
			fi ;;
			if [ "$TOTYPE" == s3 ] && ! ([ "$AWS_ENDPOINT_URL2" ])
			then
				echo "AWS_ENDPOINT_URL2 must be set!"
				exit 1
			fi
		*) $DEBUG && echo "FROMTYPE: $FROMTYPE not supported!" ; exit 1 ;;
	esac
	case "$TOTYPE" in
		s3|file)
			$DEBUG && echo "TOTYPE: $TOTYPE"
		s3)
			if ! ([ "$AWS_ENDPOINT_URL" ] && [ "$AWS_PROFILE" ])
			then
				echo "AWS_ENDPOINT_URL and AWS_PROFILE must be set!"
				exit 1
			fi ;;
		*) $DEBUG && echo "TOTYPE: $TOTYPE not supported!" ; exit 1 ;;
	esac
fi


# ============= END OF VARIABLE INITIALIZATION AND OPTION PARSING =================

_runsql() {
	su - postgres -c "psql $DVNDB -c \"$1\""
}

_getpath() {
	TYPE=$1
	NAME=$2
	case "$TYPE" in
		file)
			$ASADMIN list-jvm-options | \
			grep 'files\..*\.directory=' | \
			sed 's/.*\.\([^.]*\)\.directory=\(.*\)/\1 \2/' \
			grep "^$NAME " | \
			cut -d' ' -f2 ;;
		s3) ;;
	esac
}

# ============= ACTUAL MOVING OF OBJECTS STARTS HERE ===================

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
		#ID=`_runsql "SELECT ds1.id FROM dataset ds1 NATURAL JOIN dvobject dvo1 JOIN (datafile df2 NATURAL JOIN dvobject dvo2) ON ds1.id=dvo2.owner_id
		#             WHERE ds1.id IN (SELECT DISTINCT owner_id FROM dvobject WHERE storageidentifier LIKE '$FROM://%') GROUP BY ds1.id,dvo1.identifier"`
		echo "You MUST specify an ID for a dataset."
		exit 1
	else
		ID=$OBJ
	fi
	$DEBUG && echo "getting storageidentifier path, frompath and topath for dataset"
	datasetpath=`_runsql "SELECT REPLACE(REPLACE(storageidentifier,'$FROM://',''),'$TO://','') FROM dvobject WHERE id=$ID" | head -n3 | tail -n1 | sed 's/ *//g'`
	frompath=`$ASADMIN list-jvm-options | grep 'files\..*\.directory=' | sed 's/.*\.\([^.]*\)\.directory=\(.*\)/\1 \2/' | grep "^$FROM " | cut -d' ' -f2`
	topath=`$ASADMIN list-jvm-options | grep 'files\..*\.directory=' | sed 's/.*\.\([^.]*\)\.directory=\(.*\)/\1 \2/' | grep "^$TO " | cut -d' ' -f2`
	
	echo "setting storageidentifier for dataset $OBJ (id $ID) from $FROM to $TO"
	_runsql "UPDATE dvobject SET storageidentifier=REPLACE(storageidentifier,'$FROM://','$TO://') WHERE id=$ID"
	
	echo "creating storageidentifier path at $TO if necessary"
	case $TOTYPE in
		file)
			mkdir -p $topath/$datasetpath
			chown dataverse.dataverse $topath/$datasetpath ;;
		s3) ;;
#			echo aws mkdir $topath/$datasetpath ;;
		*) exit 1;;
	esac
	
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
		$DEBUG && echo "getting storageidentifier path for dataset"
		datasetpath=`_runsql "SELECT REPLACE(REPLACE(ds.storageidentifier,'$FROM://',''),'$TO://','') FROM dvobject ds, dvobject f WHERE f.id=$ID AND ds.id=f.owner_id " | head -n3 | tail -n1 | sed 's/^ *//;s/ *$//'`
	else
		datasetpath=$5
	fi
	if [ -z "$6" ]
	then
		$DEBUG && echo "getting frompath"
		frompath=`_getpath $FROMTYPE $FROM`
	else
		frompath=$6
	fi
	if [ -z "$7" ]
	then
		$DEBUG && echo "getting topath"
		topath=`_getpath $TOTYPE $TO`
	else
		topath=$7
	fi
	
	echo "getting file name"
	filename=`_runsql "SELECT REPLACE(REPLACE(storageidentifier,'$FROM://',''),'$TO://','') FROM datafile NATURAL JOIN dvobject WHERE id=$ID" | head -n3 | tail -n1 | sed 's/^ *//;s/ *$//'`
	
	echo "copying file $OBJ (id $ID, filename $filename) FROM $FROM (path $frompath) to $TO (path $topath), datasetpath $datasetpath"
	if [ "$FROMTYPE" = "file" ] && [ "$TOTYPE" = "file" ]
	then
		rsync --inplace $frompath/$datasetpath/$filename $topath/$datasetpath/$filename
	elif [ "$FROMTYPE" = "file" ] && [ "$TOTYPE" = "s3" ]
	then
		aws --endpoint-url "$AWS_ENDPOINT_URL" s3 --profile "$AWS_PROFILE" \
			cp "$frompath/$datasetpath/$filename" "s3://$AWS_PROFILE/$datasetpath/"
	elif [ "$FROMTYPE" = "s3" ] && [ "$TOTYPE" = "file" ]
	then
		echo aws --endpoint-url "$AWS_ENDPOINT_URL" s3 --profile "$AWS_PROFILE" \
			cp "s3://$AWS_PROFILE/$datasetpath/$filename" $topath/$datasetpath/
	elif [ "$FROMTYPE" = "s3" ] && [ "$TOTYPE" = "s3" ]
	then
		aws --endpoint-url "$AWS_ENDPOINT_URL" s3 --profile "$AWS_PROFILE" \
			cp "s3://$AWS_PROFILE/$datasetpath/$filename" $TMPPATH/
		aws --endpoint-url "$AWS_ENDPOINT_URL2" s3 --profile "$AWS_PROFILE" \
			cp "$TMPPATH/$filename" "s3://$AWS_PROFILE/$datasetpath/"
#		aws --endpoint-url "$AWS_ENDPOINT_URL" s3 --profile "$AWS_PROFILE" \
#			cp "s3://$AWS_PROFILE/$datasetpath/$filename" "s3://$AWS_PROFILE/$datasetpath/"
	else 
		echo "$FROMTYPE to $TOTYPE movement is not implemented yet."
		exit 1
	fi
	
	echo "setting storageidentifier for file $OBJ (id $ID) from $FROM to $TO"
	_runsql "UPDATE dvobject SET storageidentifier=REPLACE(storageidentifier,'$FROM://','$TO://') WHERE id=$ID"
	
	echo "removing file $OBJ (id $ID) FROM $FROM (path $frompath), datasetpath $datasetpath"
	case $FROMTYPE in
		file)
			rm $frompath/$datasetpath/$filename ;;
		s3)
			aws --endpoint-url "$AWS_ENDPOINT_URL" s3 --profile "$AWS_PROFILE" \
				rm "s3://$AWS_PROFILE/$datasetpath/$filename*" ;;
	esac
else
	_usage
fi
