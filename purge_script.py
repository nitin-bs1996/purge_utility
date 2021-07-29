import os
import boto3
import sys
from botocore.exceptions import ClientError
from bson import ObjectId
from bson.json_util import dumps
from pprint import pprint
from colorama import Fore, Back, Style
import datetime
from zipfile import ZipFile
from urllib.parse import urlparse
import argparse

try:
    iw_home = os.environ['IW_HOME']
except KeyError as e:
    print('Please source $IW_HOME/bin/env.sh before running this script')
    sys.exit(-1)

infoworks_python_dir = os.path.abspath(os.path.join(iw_home, 'apricot-meteor', 'infoworks_python'))
infoworks_temp_dir = os.path.abspath(os.path.join(iw_home, 'temp'))
sys.path.insert(0, infoworks_python_dir)
sys.path.insert(0, infoworks_temp_dir)

from infoworks.core.mongo_utils import mongodb


def get_table_details(table_id):
	try:
		table_data=mongodb.tables.find_one({'_id':ObjectId(table_id)})
		return table_data
	except e:
		print("Failed to find the table. Please provide a valid table id")
		print(str(e))
		print("exiting...")
		exit(1)

def get_source_details(source_id):
	try:
		source_data=mongodb.sources.find_one({'_id':ObjectId(source_id)})
		return source_data
	except e:
		print("Failed to find the Source corresponding to the table")
		print(str(e))
		print("exiting...")
		exit(1)

def get_environment_storage_details(environment_storage_id):
	try:
		environment_storage_data=mongodb.environment_storages.find_one({'_id':ObjectId(environment_storage_id)})
		return environment_storage_data
	except e:
		print("Failed to find the environment_storage corresponding to the table")
		print(str(e))
		print("exiting...")
		exit(1)

def get_buckets_client(bucket_name,prefix):
	session = boto3.session.Session()
	kwargs = {"Bucket": bucket_name,"Prefix":prefix,"RequestPayer":'requester'}
   # User can pass customized access key, secret_key and token as well
	s3_client = session.client('s3')
	try:
		response = s3_client.list_objects_v2(**kwargs)
		#print(response)
		buckets =[]
		objects_to_delete=[]
		if(response.get("Contents","") != ""):
			for obj in response["Contents"]:
				print(obj["Key"])
				objects_to_delete.append({"Key":obj["Key"]})
		else:
			print("No objects found under the table path ",prefix)
			exit(1)
		delete_response=s3_client.delete_objects(Bucket=bucket_name,Delete={'Objects':objects_to_delete})
		if(len(delete_response['Deleted'])>0):
			for file in delete_response['Deleted']:
				print("Deleted ",file["Key"],"successfully!")
		elif(len(delete_response['Errors'])>0):
			for file in delete_response['Errors']:
				print("Failed to delete ",file["Key"])
				print(str(file["Message"]))
			exit(1)
		else:
			pass


	except ClientError:
		print("Couldn't get buckets.")
		raise




if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Purge Utility to purge table data')
	parser.add_argument('--table_id',type=str,help='table_id of table data to be purged')
	args = parser.parse_args()
	table_data=get_table_details(args.table_id)
	print("table base path:",table_data.get("target_base_path",""))
	print("source:",table_data.get("source",""))
	source_data=get_source_details(table_data.get("source",""))
	print("environment_storage_id",source_data.get("environment_storage_id",""))
	environment_storage_id=source_data.get("environment_storage_id","")
	print("table state:",table_data.get("state",""))
	environment_storage_details=get_environment_storage_details(environment_storage_id)
	print("Environment Scheme:",environment_storage_details.get("storage_authentication",{}).get("scheme",{}))
	print("S3 Bucket name:",environment_storage_details.get("storage_authentication",{}).get("bucket_name",{}))
	table_path=table_data.get("target_base_path","")+'/merged/'
	bucket_name=environment_storage_details.get("storage_authentication",{}).get("bucket_name",{})
	get_buckets_client(bucket_name,table_path.lstrip('/'))