#!/usr/bin/env python3

import configparser
import psycopg2
import sys, os, io, re
import pprint, argparse, subprocess
from stat import *
from config import (ConfigSectionMap)
from database import (query_database, get_last_timestamp, record_datafile_status, get_datafile_status, create_database_connection)
from storage import (open_dataverse_file)
import shutil
import yaml, boto, boto.s3.connection
#from boto.s3.connection import S3Connection
import boto3
from var_dump import var_dump


GLASSFISH_DIR=os.getenv("GLASSFISH_DIR", "/usr/local/payara5")
ASADMIN=GLASSFISH_DIR+"/bin/asadmin"

### list dataverses/datasets/datafiles in a storage
### TODO: display some statistics
def getList(args):
	if args['type']=='storage':
		return getStorageDict()
	if args['type']=='dataverse':
		q="""SELECT id, alias, description FROM dataverse NATURAL JOIN dvobject WHERE true"""
		if args['ids'] is not None:
			q+=" AND id in ("+args['ids']+")"
		if args['ownerid'] is not None:
			q+=" AND owner_id="+str(args['ownerid'])
		elif args['ownername'] is not None:
			q+=" AND ds1.id IN (SELECT DISTINCT id FROM dvobject WHERE owner_id IN (SELECT id FROM dataverse WHERE alias='"+args['ownername']+"'"+"))"
		if args['storage'] is not None:
			q+=""" AND id IN
			       (SELECT DISTINCT owner_id FROM dataset NATURAL JOIN dvobject WHERE storageidentifier LIKE '"""+args['storage']+"""://%')"""
	elif args['type']=='dataset':
		q="""SELECT ds1.id, dvo1.identifier, sum(filesize) FROM dataset ds1 NATURAL JOIN dvobject dvo1 JOIN (datafile df2 NATURAL JOIN dvobject dvo2) ON ds1.id=dvo2.owner_id
		     WHERE true"""
		end=" GROUP BY ds1.id,dvo1.identifier"
		if args['ownerid'] is not None:
			q+=" AND dvo1.owner_id="+str(args['ownerid'])
		elif args['ownername'] is not None:
			q+=" AND ds1.id IN (SELECT DISTINCT id FROM dvobject WHERE owner_id IN (SELECT id FROM dataverse WHERE alias='"+args['ownername']+"'"+"))"
		if args['ids'] is not None:
			q+=" AND ds1.id in ("+args['ids']+")"
		if args['storage'] is not None:
			q+=" AND ds1.id IN (SELECT DISTINCT owner_id FROM dvobject WHERE storageidentifier LIKE '"+args['storage']+"://%')"
		q+=end
	elif args['type']=='datafile' or args['type'] is None:
		q="SELECT id, directorylabel, label, filesize, owner_id FROM datafile NATURAL JOIN dvobject NATURAL JOIN filemetadata WHERE true"
		if args['ids'] is not None:
			q+=" AND id in ("+args['ids']+")"
		if args['ownerid'] is not None:
			q+=" AND owner_id="+str(args['ownerid'])
		elif args['ownername'] is not None:
			print("Sorry, --ownername not implemented yet for files")
			exit(1)
			# q+= TODO
		if args['storage'] is not None:
			q+=" AND storageidentifier LIKE '"+args['storage']+"://%' ORDER BY owner_id"
	print(q)
	records=get_records_for_query(q)
	return records

def ls(args):
	records=getList(args)
	for r in records:
		print(r)

def changeStorageInDatabase(destStorageName,id):
	query="UPDATE dvobject SET storageidentifier=REGEXP_REPLACE(storageidentifier,'^[^:]*://',%s||'://') WHERE id=%s"
	print("updating database: "+query+" "+str((destStorageName,id)))
	sql_update(query,(destStorageName,id))

def destinationFileChecks(dst,destStoragePath,filesize):
	storageStats=os.statvfs(destStoragePath)
	if storageStats.f_bfree*storageStats.f_bsize < filesize+1000000000: # we make sure there is at least 1000MB free space after copying the file  
		print(f"There is not enough disk space to safely copy object {row[0]}. Skipping.")
		return False
	dstDir=re.sub('/[^/]*$','',dst)
	if not os.path.exists(dstDir):
		print("creating "+dstDir)
		os.mkdir(dstDir)
	return True

def moveFileChecks(row,path,fromStorageName,destStorageName):
	storageDict=getStorageDict()
	src=path[0]+path[1]
	dst=storageDict[destStorageName]['path']+path[1]
	id=str(row[0])
	if src==dst:
		print(f"Skipping object {id}, as already in storage "+destStorageName)
		return None,None
	if not os.path.exists(src):
		print("Skipping non-existent source file: "+src)
		return None,None
	if not destinationFileChecks(dst,destStoragePath,row[3]):
		return None,None
	return src,dst

def moveFileFromFileToFile(row,path,fromStorageName,destStorageName):
	src,dst = moveFileChecks(row,path,fromStorageName,destStorageName)
	if src is None or dst is None:
		return
	print("copying from", src, "to", dst)
	shutil.copyfile(src, dst)
	changeStorageInDatabase(destStorageName,row[0])
	print("removing original file "+src)
	os.remove(src)

def getS3Config(storageName):
	try:
		with open(storageName+'.yaml', 'r') as fi:
			return yaml.load(fi)
	except Exception as e:
		print(f"There was a problem parsing {storageName}.yaml")
		var_dump(e)
		os.exit(1)

s3conns={}
def getS3Connection(storageName):
	global s3conns
	if storageName in s3conns:
		return s3conns[storageName]
	
	config=getS3Config(storageName)
	region = boto.s3.S3RegionInfo(name=storageName, 
		endpoint=config['endpoint_url'],
		connection_cls=S3Connection,)
	conn = S3Connection(
		aws_access_key_id = config['access_key_id'],
		aws_secret_access_key = config['secret_access_key'],)

#	conn = boto.connect_s3(
#		endpoint = config['endpoint_url'],
#		#is_secure=False,               # uncomment if you are not using ssl
#		calling_format = boto.s3.connection.OrdinaryCallingFormat(),
#		)
	s3conns[storageName]=conn
	return conn

s3buckets={}
def getS3Bucket(storageName):
	global s3buckets
	if storageName in s3buckets:
		return s3buckets[storageName]
	
	config=getS3Config(storageName)
	for bucket in conn.get_all_buckets():
		print("{name}\t{created}".format(
			name = bucket.name,
			created = bucket.creation_date,
		))
		if bucket.name==config['bucket']:
			myBucket=bucket
	s3buckets[storageName]=bucket
	return bucket

def moveFileFromS3ToFile(row,path,fromStorageName,destStorageName):
	storageDict=getStorageDict()
	id=str(row[0])
	dst=storageDict[destStorageName]['path']+"/"+path[1]
	if not destinationFileChecks(dst,storageDict[destStorageName]['path'],row[3]):
		return
	print(f"copying from {fromStorageName}://{path[1]} to {dst}")
	bucket=getS3Connection(fromStorageName)
	key = bucket.get_key(path[1])
	key.get_contents_to_filename(dst)
	print(f"removing original file {fromStorageName}://{path[1]}")
	bucket.delete_key(path[1])

def moveFile(row,path,fromStorageName,destStorageName):
	storageDict=getStorageDict()
	if storageDict[fromStorageName]["type"]=='file' and storageDict[destStorageName]["type"]=='file':
		moveFileFromFileToFile(row,path,fromStorageName,destStorageName)
	elif storageDict[fromStorageName]["type"]=='s3' and storageDict[destStorageName]["type"]=='file':
		moveFileFromS3ToFile(row,path,fromStorageName,destStorageName)
	else:
		print(f"Moving files from {storageDict[fromStorageName]['type']} to {storageDict[destStorageName]['type']} stores is not supported yet")

def mv(args):
	if args['to_storage'] is None:
		print("--to-storage is missing")
		exit(1)
	storages=getStorageDict()
	if args['to_storage'] not in storages:
		print(args['to_storage']+" is not a valid storage. Valid storages:")
		pprint.PrettyPrinter(indent=4,width=10).pprint(storages)
		exit(1)
	objectsToMove=getList(args)
	if args['type']=='datafile':
		filePaths=get_filepaths(idlist=[str(x[0]) for x in objectsToMove],separatePaths=True)
		for row in objectsToMove:
			moveFile(row,filePaths[row[0]],args['storage'],args['to_storage'])
	else:
		for row in objectsToMove:
			changeStorageInDatabase(args['to_storage'],row[0])
			if args['recursive']:
				newargs={
					'type':'dataverse',
					'ownerid':row[0],
					'to_storage':args['to_storage'],
					'ids': None,
					'storage':args['storage'],
					'recursive': True}
				mv(newargs)
				newargs.update({'type':'dataset'})
				mv(newargs)
				newargs.update({'type':'datafile'})
				mv(newargs)

### this is for checking that the files in the database are all there on disk where they should be
def fsck(args):
	if args['storage'] is not None or args['ownerid'] is not None or args['ids'] is not None:
		filesToCheck=getList(args)
		filepaths=get_filepaths([str(x[0]) for x in filesToCheck])
	else:
		filepaths=get_filepaths()
	#print filepaths
	#print "Will check "+str(len(filepaths))+" files."
	checked, errors = 0, 0
	for f in filepaths:
		try:
			if not S_ISREG(os.stat(filepaths[f]).st_mode):
				print(filepaths[f] + " is not a normal file")
				errors+=1
		except:
			print("cannot stat", filepaths[f], "  id:", f)
			errors+=1
		checked+=1
	print("Checked", checked, "objects, errors:", errors)

#def getStorages():
#	out=os.popen("./list_storages.sh").read()
##	print out
#	result={}
#	for line in out.splitlines():
#		l=line.split(' ')
#		result[l[0]]={'path': l[1]+'/', 'free': l[2], 'freePercent': l[3]}
#	return result

storageDict=None
def getStorageDict():
	global storageDict
	if storageDict==None:
		storageDict=calculateStorageDict()
	return storageDict

def calculateStorageDict():
	storageTypeOutput=subprocess.run(ASADMIN+" list-jvm-options | grep 'files\..*\.type='", shell=True, check=True, capture_output=True, text=True).stdout.splitlines()
	storageDict={}
	for x in storageTypeOutput:
		sp=x.split('=')
		storage=re.match(r'-Ddataverse\.files\.(?P<name>\w+)\.type=(?P<type>\w+)', x).groupdict()
		storageDirectoryOutput=subprocess.run(ASADMIN+" list-jvm-options | egrep \"files\.("+storage['name']+")\.directory=\"", shell=True, capture_output=True, text=True).stdout.splitlines()
		if len(storageDirectoryOutput)==1:
			o=storageDirectoryOutput[0]
			storage["path"]=o.replace("-Ddataverse.files."+storage['name']+".directory=","")
			storage["freeMegabytes"]=subprocess.run("df -m "+storage["path"]+" --output=avail| tail -n1", shell=True, capture_output=True, text=True).stdout.splitlines()[0]
			storage["freePercent"]=subprocess.run("df "+storage["path"]+" --output=pcent|tail -n1", shell=True, capture_output=True, text=True).stdout.splitlines()[0]
		elif len(storageDirectoryOutput)==0:
			storage["path"]=None
		else:
			sys.exit(1)
		storageDict[storage['name']]=storage
	return storageDict

def get_records_for_query(query):
	dataverse_db_connection = create_database_connection()
	cursor = dataverse_db_connection.cursor()
	cursor.execute(query)
	records = cursor.fetchall()
	dataverse_db_connection.close()
	return records

def sql_update(query, params):
	dataverse_db_connection = create_database_connection()
	cursor = dataverse_db_connection.cursor()
	cursor.execute(query, params)
	dataverse_db_connection.commit() 
	dataverse_db_connection.close()

def get_filepaths(idlist=None,separatePaths=False):
	storages=getStorageDict()

	query="""SELECT f.id, REGEXP_REPLACE(f.storageidentifier,'^([^:]*)://.*','\\1'), REGEXP_REPLACE(s.storageidentifier,'^[^:]*://','') || '/' || REGEXP_REPLACE(f.storageidentifier,'^[^:]*://','')
	         FROM dvobject f, dvobject s
	         WHERE f.dtype='DataFile' AND f.owner_id=s.id"""
	if idlist is not None:
		if not idlist: # there is an idlist, but it is empty
			return {}
		query+=" AND f.id IN("+','.join(idlist)+")"
	records=get_records_for_query(query)
	result={}
	for r in records:
		if separatePaths:
			result.update({r[0] : (storages[r[1]]['path'],r[2])})
		else:
			result.update({r[0] : storages[r[1]]['path']+r[2]})
	return result

def main():
	commands={
		"list" : ls,
		"move" : mv,
		"fsck" : fsck,
	}
	types=["storage","dataverse","dataset","datafile"]
#	print commands.keys()

	argv = sys.argv[2:]
	ap = argparse.ArgumentParser()
	ap.add_argument("command", choices=commands.keys(), help="what to do")
	ap.add_argument("-n", "--name", required=False, help="name of the object")
	ap.add_argument("-d", "--ownername", required=False, help="name of the containing/owner object")
	ap.add_argument("-i", "--ids", required=False, help="id(s) of the object(s), comma separated")
	ap.add_argument("--ownerid", required=False, help="id of the containing/owner object")
	ap.add_argument("-t", "--type", choices=types, required=False, help="type of objects to list/move")
	ap.add_argument("-s", "--storage", required=False, help="storage to list/move items from")
	ap.add_argument("--to-storage", required=False, help="move to the datastore of this name, required for move")
	ap.add_argument("-r", "--recursive", required=False, action='store_true', help="make move recursive")
	args = vars(ap.parse_args())
#	opts, args = getopt.getopt(argv, 'type:id:name:')

	print(args)
	commands[args['command']](args)


if __name__ == "__main__":
	main()


