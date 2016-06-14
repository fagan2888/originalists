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

def trainSaveSQL(trainData):
	import datetime, pandas as pd
	from sqlalchemy import create_engine
	engine = create_engine('postgresql+psycopg2://Original:tolistbtGU!@teamoriginal.ccc95gjlnnnc.us-east-1.rds.amazonaws.com:5432/WmataData') #opens the engine to WmataData
        #the line below creates a table name starting with WMATA and then containing the date and time information, with each day/hour/minute/second taking two characters
	tableName='WMATA'+str(datetime.datetime.now().month)+'-'+str(datetime.datetime.now().day).rjust(2,'0')+'_'+str(datetime.datetime.now().hour).rjust(2,'0')+'_'+str(datetime.datetime.now().minute).rjust(2,'0')+'_'+str(datetime.datetime.now().second).rjust(2,'0')
	trainFrame=pd.DataFrame('-', index=range(len(trainData)), columns=['Car','Loc','Lin','Des','Min','Gro']) #creates trainFrame, the DataFrame to send to the SQL server
	for iter in range(len(trainData)): #for all the trains in trainData
         for colName in ['Car','LocationCode','Line','DestinationCode','Min','Group']: #select the six relevant fields
             trainFrame.loc[iter][colName[:3]]=[trainData[iter][colName]] #and fill in the relevant data  
	trainFrame.to_sql(tableName, engine, if_exists='append') #send trainFrame to the SQL server

def recordTrainsSQL(timeMin,intervalSec): #records for timeMin minutes, about ever intervalSec seconds
	import time
	startTime=time.time()
	while time.time()<(startTime+60*timeMin): #runs for timeMin minutes
		stepStart=time.time()
		trainSaveSQL(JSONfromMetro(getMetroStatus())) #save the current train data
		stepTime=time.time()-stepStart #calculates the time this step took
		time.sleep(intervalSec-stepTime) #wait a few seconds

#Stations from Rosslyn to Stadium-Armory (Silver Orange Blue) to serve as input for LineStat
SOBLine=['C05', 'C04', 'C03', 'C02', 'C01', 'D01', 'D02', 'D03', 'D04', 'D05', 'D06', 'D07', 'D08']