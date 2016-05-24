def getMetroStatus():
	import http.client, urllib.request, urllib.parse, urllib.error, base64, time
	headers = {
    # Request headers
    'api_key': '6b700f7ea9db408e9745c207da7ca827',}
	params = urllib.parse.urlencode({})
	try:
		conn = http.client.HTTPSConnection('api.wmata.com')
		conn.request("GET", "/StationPrediction.svc/json/GetPrediction/All?%s" % params, "{body}", headers)
		response = conn.getresponse()
		data = response.read()
		return str(data) #returns the data as a string rather than raw bytes
		conn.close()
	except Exception as e:
		print("[Errno {0}] {1}".format(e.errno, e.strerror))

def JSONfromMetro(trainString): #converts the string into a dictionary file
	import json, re
	fixSlash=re.compile(r'\\') #this line and the next remove triple-slashes, which screw up the json module
	fixedTrainString=fixSlash.sub('',trainString)
	trainJSON=json.loads(fixedTrainString[2:-2]+"}") #slightly adjusts the string to put it in json form
	return trainJSON['Trains']

def trainSave(trainData):
	import csv, datetime
	csvFile = open('WMATA_CSV'+str(datetime.datetime.now().month)+'_'+str(datetime.datetime.now().day)+'_'+str(datetime.datetime.now().hour)+'_'+str(datetime.datetime.now().minute)+'_'+str(datetime.datetime.now().second)+'.csv', 'w', newline='') #creates a new  CSW file named WMATA_CSV(time)
	trainCSVWriter = csv.writer(csvFile)
	for iter in range(len(trainData)): #for all the lines in trainData
		trainLine=[trainData[iter]['Car'],trainData[iter]['LocationCode'],trainData[iter]['Line'],trainData[iter]['DestinationCode'],trainData[iter]['Min']] #extract the relevant data
		trainCSVWriter.writerow(trainLine) #and write it to the CSV File
	csvFile.close()

def recordTrains(timeMin,intervalSec, outputDirectory): #records for timeMin minutes, about ever intervalSec seconds
	import time, os
	os.chdir(outputDirectory) #puts all the data into a folder. For me it's '/Users/aaronmargolis/Documents/WMATA_Data'
	startTime=time.time()
	while time.time()<(startTime+60*timeMin): #runs for timeMin minutes
		stepStart=time.time()
		trainSave(JSONfromMetro(getMetroStatus())) #save the current train data
		stepTime=time.time()-stepStart #calculates the time this step took
		time.sleep(intervalSec-stepTime) #wait a few seconds
