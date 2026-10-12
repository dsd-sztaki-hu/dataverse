#!/usr/bin/python3

import psycopg2, psycopg2.extras
import sys, os, io, re, requests, argparse
from database import (get_records_for_query, sql_update)
from config import (ConfigSectionMap)

DEFAULT_VERBOSITY=0
api_token = ConfigSectionMap("Dataverse")['apitoken']

def ls(s):
	if not s['sizeinbytes']: s['sizeinbytes']=0
	print(f"Alias: {s['alias']:<20}    Id: {s['id']:8d}    Quota: {s['allocation']/1000000:10.0f}M    Size: {s['sizeinbytes']/1000000:10.0f}M    Free: {(s['allocation']-s['sizeinbytes'])*100/s['allocation']:4.0f}%")

def verify(s):
	resp=requests.get(f"http://localhost:8080/api/dataverses/{s['alias']}/storagesize",headers={"X-Dataverse-key":api_token}).json()
	verbosity=DEFAULT_VERBOSITY
	if resp['status']=='OK' and resp['data']['message'].startswith('Total size of the files stored in this dataverse: '):
		actualsize=int(re.match(r'^Total size of the files stored in this dataverse: ([0-9,]+) bytes$', resp['data']['message']).group(1).replace(',',''))
		if actualsize==None:
			print(resp)
			print(actualsize)
			print(re.match(r'^Total size of the files stored in this dataverse: ([0-9,]+) bytes$', resp['data']['message']).group(1).replace(',',''))
			exit(2)
		if actualsize!=s['sizeinbytes']:
			if s['sizeinbytes']==None:
				s['sizeinbytes']=0
			diff=actualsize-s['sizeinbytes']
			if actualsize==0 and s['sizeinbytes']:
				percent='Inf'
			elif actualsize==0 and not s['sizeinbytes']:
				percent=0
			else:
				percent=diff*100/actualsize
			if abs(percent)<1 or diff<10000000:
				if verbosity>0: print("Difference is small, inoring.")
			else:
				print(f"!!!!!!!!!!!!!!!  Difference is BIG for {s['alias']} !!!!!!!!!!!!!!!!!")
				verbosity+=1
			if verbosity>0:
				print(f"Alias: {s['alias']:<20}    Id: {s['id']:8d}    Quota: {s['allocation']/1000000:10.0f}M    Size: {s['sizeinbytes']/1000000:10.0f}M    Actual size: {actualsize/1000000:10.0f}M ")
				print(f"The difference is {diff/1000000:10.0f}M ({percent:.2f}%).")
	else:
		print('Invalid response: '+resp)
		exit(1)
#	exit(0)

def fix(s):
	print("NOT IMPLEMENTED YET")
	exit(3)

COMMANDS={
	"list" : ls,
	"ls" : ls,
	"verify" : verify,
	"fix" : fix,
}

def main():
	ap = argparse.ArgumentParser()
	ap.add_argument("command", choices=COMMANDS.keys(), help="what to do")
	ap.add_argument("-i", "--ids", required=False, help="id(s) of the object(s), comma separated")
	ap.add_argument("--debug", required=False, action='store_true', help="print debug messages")
	args = vars(ap.parse_args())
	
	if args['ids'] is not None:
		idq="AND d.id in ("+args['ids']+")"
	else:
		idq=""
	
	if args['debug']:
		global DEFAULT_VERBOSITY
		DEFAULT_VERBOSITY+=1
	
	################## processing args done ####################
	
	nondataversequotas=get_records_for_query(f"SELECT id FROM dvobject d WHERE id IN (SELECT definitionpoint_id FROM storagequota) {idq} AND dtype != 'Dataverse'")
	if len(nondataversequotas)!=0:
		print("WARNING! There are some non-dataverse quota'd objects. I will not be able to check them:")
		print(nondataversequotas)
	
	#storageuses=get_records_for_query("SELECT * FROM storageuse ORDER BY dvobjectcontainer_id")
	storageuses=get_records_for_query(f"""
		SELECT d.id, alias, allocation, sizeinbytes
		FROM storagequota JOIN storageuse ON definitionpoint_id=dvobjectcontainer_id JOIN dataverse d ON definitionpoint_id=d.id
		WHERE true {idq}
		ORDER BY d.id
		OFFSET 0""")
	
	args['command']=COMMANDS[args['command']]
	for s in storageuses:
		args['command'](s)

if __name__ == "__main__":
	main()

