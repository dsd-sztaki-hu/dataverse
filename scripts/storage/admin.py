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
import yaml
import boto3
import tempfile
from var_dump import var_dump
from icecream import ic

GLASSFISH_DIR=os.getenv("GLASSFISH_DIR", "/usr/local/payara5")
ASADMIN=GLASSFISH_DIR+"/bin/asadmin"
DEBUG_ENABLED=True
### list dataverses/datasets/datafiles in a storage
### TODO: display some statistics
def getList(args):
	if args['type']=='storage':
		return getStorageDict()
	if args['type']=='dataverse':
		q="""SELECT id, alias, description, storagedriver FROM dataverse NATURAL JOIN dvobject WHERE true"""
		if args['ids'] is not None:
			q+=" AND id in ("+args['ids']+")"
		if args['ownerid'] is not None:
			q+=" AND owner_id="+str(args['ownerid'])
		elif args['ownername'] is not None:
			q+=" AND id IN (SELECT DISTINCT id FROM dvobject WHERE owner_id IN (SELECT id FROM dataverse WHERE alias='"+args['ownername']+"'"+"))"
		if args['storage'] is not None:
			q+=" AND storagedriver='"+str(args['storage'])+"'"
	elif args['type']=='dataset':
		q="""SELECT ds1.id, dvo1.identifier, sum(filesize), dvo1.storageidentifier FROM dataset ds1 NATURAL JOIN dvobject dvo1 JOIN (datafile df2 NATURAL JOIN dvobject dvo2) ON ds1.id=dvo2.owner_id
		     WHERE true"""
		end=" GROUP BY ds1.id,dvo1.identifier,dvo1.storageidentifier"
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
		q="SELECT dvo.id, directorylabel, label, filesize, owner_id, dvo.storageidentifier FROM datafile NATURAL JOIN dvobject dvo JOIN filemetadata fm ON dvo.id=fm.datafile_id WHERE true"
		if args['ids'] is not None:
			q+=" AND dvo.id in ("+args['ids']+")"
		if args['ownerid'] is not None:
			q+=" AND owner_id="+str(args['ownerid'])
		elif args['ownername'] is not None:
			print("Sorry, --ownername not implemented yet for files")
			exit(1)
			# q+= TODO
		if args['storage'] is not None:
			q+=" AND storageidentifier LIKE '"+args['storage']+"://%' ORDER BY owner_id"
	
	records=get_records_for_query(q)
	if args['recursive']:
		args.update({'command':'getList'})
		recurseOut=[]
		for r in records:
			recurseOut+=recurse(args,r[0])
		records+=recurseOut
	return records

def ls(args):
	records=getList(args)
	for r in records:
		print(r)
	return records

def changeStorageInDatabase(destStorageName,id,type='datafile'):
	storages=getStorageDict()
	if type=='dataverse':
		query="UPDATE dataverse SET storagedriver=%s "
		paramTuple=(destStorageName,id)
	else:
		query="UPDATE dvobject SET storageidentifier=REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(storageidentifier,'^[^:]*://',%s||'://'),'://[0-9a-zA-Z_-]*:','://'),'://','://'"
		try:
			bucketName=(getS3Bucket(destStorageName,silent=True).name)+":"
			query+="||%s"
			paramTuple=(destStorageName,bucketName,id)
		except: # Exception as e:
			paramTuple=(destStorageName,id)
			#raise e
		query+=") "
	query+="WHERE id=%s"
	#	query=f"storageidentifier=REPLACE(storageidentifier,'${fromStorageName}://','{destStorageName}://')"
#	ic(query,paramTuple)
	print("updating database: "+(query%paramTuple))
	#	exit(0)
	sql_update(query,paramTuple)


def destinationFileChecks(dst,destStoragePath,filesize,id):
	storageStats=os.statvfs(destStoragePath)
	if storageStats.f_bfree*storageStats.f_bsize < filesize+1000000000: # we make sure there is at least 1000MB free space after copying the file  
		print(f"There is not enough disk space to safely copy object {id}. Skipping.")
		return False
	dstDir=re.sub('/[^/]*$','',dst)
	if not os.path.exists(dstDir):
		print("creating "+dstDir)
		os.makedirs(dstDir)
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
	if not destinationFileChecks(dst,destStoragePath,row[3],row[0]):
		return None,None
	return src,dst

def move_or_copy_file_from_file_to_file(row,path,fromStorageName,destStorageName,move):
	src,dst = moveFileChecks(row,path,fromStorageName,destStorageName)
	if src is None or dst is None:
		return
	print(f"copying from {src} to {dst}")
	shutil.copyfile(src, dst)
	if move:
		changeStorageInDatabase(destStorageName,row[0])
		print(f"removing original file {src}")
		os.remove(src)

def getS3Config(storageName,silent=False):
	try:
		with open(storageName+'.yaml', 'r') as fi:
			return yaml.load(fi, Loader=yaml.FullLoader)
	except Exception as e:
		if not silent:
			print(f"ERROR: There was a problem parsing {storageName}.yaml")
			var_dump(e)
			exit(1)

s3conns={}
def getS3Connection(storageName,silent=False):
	global s3conns
	if storageName in s3conns:
		return s3conns[storageName]
	
	config=getS3Config(storageName,silent)
	debug(config)
	conn = boto3.resource(
		's3',
		endpoint_url=config['endpoint_url'],
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
def getS3Bucket(storageName,silent=False):
	return getS3BucketAndConnection(storageName,silent)[0]

def getS3BucketAndConnection(storageName,silent=False):
	global s3buckets, s3conns
	ic(storageName,silent,s3buckets,s3conns)
	if storageName in s3buckets:
		return s3buckets[storageName],s3conns[storageName]
	conn=getS3Connection(storageName,silent)
	config=getS3Config(storageName,silent)
	for bucket in conn.buckets.all():
		print("{name}\t{created}".format(
			name = bucket.name,
			created = bucket.creation_date,
		))
		if bucket.name==config['bucket']:
			myBucket=bucket
	s3buckets[storageName]=myBucket
	return myBucket, conn

def getS3BucketAndClient(storageName):
	myBucket, conn = getS3BucketAndConnection(storageName)
	return myBucket, conn.meta.client

def move_or_copy_file_from_s3_to_file(row,path,fromStorageName,destStorageName,move):
	storageDict=getStorageDict()
	id=str(row[0])
	dst=storageDict[destStorageName]['path']+"/"+path[1]
	if not destinationFileChecks(dst,storageDict[destStorageName]['path'],row[3],id):
		return
	bucket,client=getS3BucketAndClient(fromStorageName)
	key=path[1]
	print(f"copying from {fromStorageName}://{key} to {dst}")
#	ic(bucket,bucket.name)
#	debug(client.list_objects(Bucket=bucket.name))
	client.download_file(Filename=dst,Bucket=bucket.name,Key=key)
	if move:
		changeStorageInDatabase(destStorageName,id)
		print(f"removing original file {fromStorageName}://{key}")
		client.delete_object(Bucket=bucket.name,Key=key)

def move_or_copy_file_from_file_to_s3(row,path,fromStorageName,destStorageName,move):
	storageDict=getStorageDict()
	id=str(row[0])
#	ic(storageDict[destStorageName]['path'],path[1])
	src=storageDict[fromStorageName]['path']+"/"+path[1]
	bucket,client=getS3BucketAndClient(destStorageName)
	key=path[1]
	print(f"copying from {src} to {destStorageName}://{key}")
#	ic(bucket,bucket.name)
#	debug(client.list_objects(Bucket=bucket.name))
	client.upload_file(Filename=src,Bucket=bucket.name,Key=key)
	if move:
		changeStorageInDatabase(destStorageName,id)
		print(f"removing original file {src}")
		os.remove(src)

def move_or_copy_file_from_s3_to_s3(row,path,fromStorageName,destStorageName,move):
	storageDict=getStorageDict()
	id=str(row[0])
	bucket1,client1=getS3BucketAndClient(fromStorageName)
	bucket2,client2=getS3BucketAndClient(destStorageName)
	key=path[1]
	print(f"copying from {fromStorageName}://{key} to {destStorageName}://{key}")
	with tempfile.TemporaryFile() as fp:
		client1.download_fileobj(Fileobj=fp,Bucket=bucket1.name,Key=key)
		fp.seek(0)
		client2.upload_fileobj(Fileobj=fp,Bucket=bucket2.name,Key=key)
	if move:
		changeStorageInDatabase(destStorageName,id)
		print(f"removing original file {src}")
		client.delete_object(Bucket=bucket1.name,Key=key)


def move_or_copy_file(row,path,fromStorageName,destStorageName,move):
#	debug(f"movefile({row},{path},{fromStorageName},{destStorageName})")
#	try:
		storageDict=getStorageDict()
		if storageDict[fromStorageName]["type"]=='file' and storageDict[destStorageName]["type"]=='file':
			move_or_copy_file_from_file_to_file(row,path,fromStorageName,destStorageName,move)
		elif storageDict[fromStorageName]["type"]=='s3' and storageDict[destStorageName]["type"]=='file':
			move_or_copy_file_from_s3_to_file(row,path,fromStorageName,destStorageName,move)
		elif storageDict[fromStorageName]["type"]=='file' and storageDict[destStorageName]["type"]=='s3':
			move_or_copy_file_from_file_to_s3(row,path,fromStorageName,destStorageName,move)
		elif storageDict[fromStorageName]["type"]=='s3' and storageDict[destStorageName]["type"]=='s3':
			move_or_copy_file_from_s3_to_s3(row,path,fromStorageName,destStorageName,move)
		elif storageDict[fromStorageName]["type"]=='swift' and storageDict[destStorageName]["type"]=='swift':
			print("Moving file to and from swift is not supported and, as dataverse swift support itself is deprecated, it may never be.")
		else:
			print(f"Moving files from {storageDict[fromStorageName]['type']} to {storageDict[destStorageName]['type']} stores is not supported yet")
#	except Exception as e:
#		print(f"moving file {row[1]} (id: {row[0]}) caused an exception: {e}")

def cp(args):
	mv_or_cp(args)

def mv(args):
	mv_or_cp(args)

def mv_or_cp(args):
	if args['to_storage'] is None:
		print("ERROR: --to-storage is missing")
		exit(1)
	storages=getStorageDict()
	if args['to_storage'] not in storages:
		print("ERROR: "+args['to_storage']+" is not a valid storage. Valid storages:")
		pprint.PrettyPrinter(indent=4,width=10).pprint(storages)
		exit(1)
	objectsToMove=getList(args)
	if args['type']=='datafile':
		filePaths=get_filepaths(idlist=[str(x[0]) for x in objectsToMove],separatePaths=True)
		for row in objectsToMove:
			move_or_copy_file(row,filePaths[row[0]],args['storage'],args['to_storage'],args['command'].__name__=='mv')
	elif args['command'].__name__=='mv':
		ic(objectsToMove)
		for row in objectsToMove:
			debug(row)
			changeStorageInDatabase(args['to_storage'],row[0],args['type'])
			recurse(args,row[0])

def get_new_args(args, id=None, ownerid=None, ownername=None, type='dataverse'):
	return {
		'id': id,
		'type': type,
		'to_storage': args['to_storage'],
		'storage': args['storage'],
		'recursive': args['recursive'],
		'command': args['command'],
		'ownerid': ownerid,
		'ownername': ownername,
		'ids': None,
	}

def recurse(args, id):
	out=[]
	if args['recursive']:
		out+=COMMANDS[args['command']](get_new_args(args, ownerid=id, type='dataverse'))
		out+=COMMANDS[args['command']](get_new_args(args, ownerid=id, type='dataset'))
		out+=COMMANDS[args['command']](get_new_args(args, ownerid=id, type='datafile'))
	return out


### this is for checking that the files in the database are all there on disk where they should be
def fsck(args):
	if args['storage'] is not None or args['ownerid'] is not None or args['ids'] is not None:
		filesToCheck=getList(args)
		filepaths=get_filepaths([str(x[0]) for x in filesToCheck])
	else:
		filepaths=get_filepaths()
	debug(filesToCheck)
	#print filepaths
	#print "Will check "+str(len(filepaths))+" files."
	storages=getStorageDict()
	checked, errors, skipped = 0, 0, 0
	for f in filepaths:
		try:
			debug('fscking: ',id=f, metadata=filepaths[f])
			if storages[filepaths[f][2]]['type']=='file':
				fp=storages[filepaths[f][2]]['path']+filepaths[f][1]
				if not S_ISREG(os.stat(fp).st_mode):
					print(filepaths[f] + " is not a normal file!")
					errors+=1
			elif storages[filepaths[f][2]]['type']=='s3':
				bucket,conn=getS3BucketAndClient(filepaths[f][2])
				#oa=client.get_object_attributes(Bucket=bucket.name,Key=filepaths[f][1],ObjectAttributes=['Checksum','ObjectSize'])
				oa=client.list_objects_v2(Bucket=bucket.name,Prefix=filepaths[f][1])['Contents'][0]
				if oa['Size']!=getList(get_new_args(args,id=f,type="datafile"))[0][3]:
					print(f"size mismatch for {filepaths[f]}  id: {f}!")
					errors+=1
				else:
					debug(f"OK {filepaths[f][1]}")
			else:
				print(f"fsck not implemented yet for {storages[filepaths[f][2]]['type']}!")
				skipped+=1
		except Exception as e:
			print(f"cannot stat {filepaths[f]}  id: {f}")
			debug(e)
			errors+=1
		checked+=1
	print(f"Total objects: {checked};  Skipped: {skipped};  Errors: {errors};  OK: {checked-skipped-errors}")

#def checkFileInFilesystem():
#	

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
	debug(storageTypeOutput)
	storageDict={}
	for x in storageTypeOutput:
		sp=x.split('=')
		storage=re.match(r'-Ddataverse\.files\.(?P<name>[0-9a-zA-Z_-]+)\.type=(?P<type>\w+)', x).groupdict()
		storageDirectoryOutput=subprocess.run(ASADMIN+" list-jvm-options | egrep \"files\.("+storage['name']+")\.directory=\"", shell=True, capture_output=True, text=True).stdout.splitlines()
		if len(storageDirectoryOutput)==1:
			o=storageDirectoryOutput[0]
			storage["path"]=o.replace("-Ddataverse.files."+storage['name']+".directory=","")
			storage["freeMegabytes"]=subprocess.run("df -m "+storage["path"]+" --output=avail| tail -n1", shell=True, capture_output=True, text=True).stdout.splitlines()[0]
			storage["freePercent"]=subprocess.run("df "+storage["path"]+" --output=pcent|tail -n1", shell=True, capture_output=True, text=True).stdout.splitlines()[0]
		elif len(storageDirectoryOutput)==0:
			storage["path"]=None
		else:
			print(f"ERROR: len(storageDirectoryOutput)=={len(storageDirectoryOutput)}, it should be 0 or 1!!!")
			exit(1)
		storageDict[storage['name']]=storage
	return storageDict

def debug(*debug,**kdebug):
	if DEBUG_ENABLED:
		if kdebug and debug:
			ic(debug,kdebug)
		elif debug:
			ic(debug)
		elif kdebug:
			ic(kdebug)

def get_records_for_query(query):
	debug(query)
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

def get_filepaths(idlist=None,separatePaths=True):
	storages=getStorageDict()

	query="""SELECT f.id, REGEXP_REPLACE(f.storageidentifier,'^([^:]*)://.*','\\1'), REGEXP_REPLACE(s.storageidentifier,'^[^:]*://','') || '/' || REGEXP_REPLACE(REGEXP_REPLACE(f.storageidentifier,'^[^:]*://',''),'^[^:]*:','')
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
			result.update({r[0] : (storages[r[1]]['path'],r[2],r[1])})
		else:
			result.update({r[0] : storages[r[1]]['path']+r[2]})
	return result

COMMANDS={
	"list" : ls,
	"ls" : ls,
	"getList" : getList,
	"move" : mv,
	"mv" : mv,
	"copy" : cp,
	"cp" : cp,
	"check" : fsck,
	"fsck" : fsck,
}

def main():
	types=["storage","dataverse","dataset","datafile"]
#	print COMMANDS.keys()

	ap = argparse.ArgumentParser()
	ap.add_argument("command", choices=COMMANDS.keys(), help="what to do")
	ap.add_argument("-n", "--name", required=False, help="name of the object")
	ap.add_argument("-d", "--ownername", required=False, help="name of the containing/owner object")
	ap.add_argument("-i", "--ids", required=False, help="id(s) of the object(s), comma separated")
	ap.add_argument("--ownerid", required=False, help="id of the containing/owner object")
	ap.add_argument("-t", "--type", choices=types, required=False, help="type of objects to list/move")
	ap.add_argument("-s", "--storage", required=False, help="storage to list/move items from")
	ap.add_argument("--to-storage", required=False, help="move to the datastore of this name, required for move")
	ap.add_argument("-r", "--recursive", required=False, action='store_true', help="make action recursive")
	args = vars(ap.parse_args())

	ic(args)
	args['command']=COMMANDS[args['command']]
	#ic(COMMANDS[args['command']].__name__)
	args['command'](args)


if __name__ == "__main__":
	main()


