import boto3
from faker import Faker
import random
import time
import json
import sys
DeliveryStreamName = 'ForcusPointLogs'
AnalysisAppName = 'FocusPointLogFilter'
client = boto3.client('kinesis')
analysisclient = boto3.client('kinesisanalytics')
fake = Faker()
maxnumbersearched = 5
waittimecreatingstream = 300
	
try:
	#List all the Kinesis Stream starting with DeliveryStreamName	
	response = client.list_streams(
		Limit = maxnumbersearched
	)
	
	#if there is no stream, create one
	streamname = response['StreamNames']	
	if DeliveryStreamName in streamname :
		print('stream ' + DeliveryStreamName + 'is ready for action')
	else:	
		print('there is no stream availble, creating one ...')
		response = client.create_stream(
			StreamName=DeliveryStreamName,
			ShardCount=1
		)
		time.sleep(waittimecreatingstream)
		print(response)
	
	#find the application and its status		
	response = analysisclient.list_applications(
		Limit = maxnumbersearched
	)
	for app in response['ApplicationSummaries']:
		if app['ApplicationName'] == AnalysisAppName:
			status = app['ApplicationStatus']
			if 'READY' == status:			
				#Get the application info
				response = analysisclient.describe_application(
					ApplicationName=AnalysisAppName
				)
				
				print(response)
				print(' ')
				appid = response['ApplicationDetail']['InputDescriptions'][0]['InputId']
				
				#start the application
				response = analysisclient.start_application(
					ApplicationName=AnalysisAppName,
					InputConfigurations=[
						{
							'Id': appid,
							'InputStartingPositionConfiguration': {
								'InputStartingPosition': 'LAST_STOPPED_POINT'
							}
						},
					]
				)
				print(response)
				print(' ')
	
except Exception as e:
	print(e)
	

	
