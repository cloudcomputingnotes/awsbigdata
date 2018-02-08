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
	
try:
	#List all the Kinesis Stream starting with DeliveryStreamName	
	response = client.list_streams(
		Limit = 5
	)
	
	#delete the steam if there is one
	streamname = response['StreamNames']
	if DeliveryStreamName in streamname:
		print('deleteing ' + DeliveryStreamName + ' ...')
		response = client.delete_stream(
			StreamName=DeliveryStreamName
		)
		print(str(response))
		
	#stop the application
	
	print('stopping ' + AnalysisAppName + ' ...')
	response = analysisclient.stop_application(
		ApplicationName=AnalysisAppName
	)
	print(response)
except Exception as e:
	print(e)
	
	

	
