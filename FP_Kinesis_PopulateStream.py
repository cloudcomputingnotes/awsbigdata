import boto3
import time
import json
import sys
import random
DeliveryStreamName = 'ForcusPointLogs'
client = boto3.client('kinesis')
	
try:
	#List all the Kinesis Stream starting with DeliveryStreamName	
	response = client.list_streams(
		Limit = 5
	)
	print(response)
	#if there is the stream, upload to the streams
	streamname = response['StreamNames']	
	if streamname :
		print('stream ' + streamname[0] + 'is ready for action')
		#while True:
		count = 0
		while count < 1000:
			with open("FocusPoint.log") as f:
				for line in f:
					key = str(random.randint(1, 9))
					servername = "WSTESTSERVER00" + key
					logdata = servername + " | " + line
					response = client.put_record(
						StreamName=DeliveryStreamName,        
						Data = logdata, 
						PartitionKey=key
					)
					print(response)
			count+=1
		
except Exception as e:
	print(e)
	
