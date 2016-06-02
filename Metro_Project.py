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
	csvFile = open('WMATA_CSV'+str(datetime.datetime.now().month)+'-'+str(datetime.datetime.now().day).rjust(2,'0')+'_'+str(datetime.datetime.now().hour).rjust(2,'0')+'_'+str(datetime.datetime.now().minute).rjust(2,'0')+'_'+str(datetime.datetime.now().second).rjust(2,'0')+'.csv', 'w', newline='') #creates a new  CSW file named WMATA_CSV(time)
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

def lineStatCSV(line, fileName,destList): #reads the status of a line and returns it as a Data Frame
	import csv, pandas as pd
	trainFile = open(fileName) #the next three lines load the train data from the file name
	trainReader=csv.reader(trainFile)
	trainData=list(trainReader)
	rowName=pd.to_datetime('2016-'+fileName[-17]+'-'+fileName[-15:-13]+' '+fileName[-12:-10]+':'+fileName[-9:-7]+':'+fileName[-6:-4])
# names the row as a timestamp with the month and day                hour                 minute                   second
	lineStat=pd.DataFrame('-',index=[rowName],columns=line)
	for station in line: #repeat the below process for every station on the line
		trainNum=0 #the below lines look for the first train going to a destination in destList
		while trainNum<len(trainData):
			if (trainData[trainNum][1]==station) and (trainData[trainNum][3] in destList):
				break
			trainNum+=1
		if trainNum<len(trainData): #If you found a train
			lineStat.loc[rowName][station]=trainData[trainNum][2]+':'+trainData[trainNum][4] #set the station status to the color and ETA of arriving train
	return lineStat


def saveLine(line,statusFiles, destList): #saves a line status over time
	import csv, pandas as pd
	lineStats=lineStatCSV(line,statusFiles[0], destList)
	for status in statusFiles[1:]:
		lineStats.concat(lineStatCSV(line,status,destList))
	return lineStats

def recordStat(lineStat,fileName): #Writes the lineStat panda to a CSV file
	import os, csv
	csvFileObj = open(fileName, 'w', newline='')
	csvWriter = csv.writer(csvFileObj)
	csvWriter.writerow([' ']+list(lineStat.columns)) #Writes down the column labels (stations)
	for rowNum in range(len(lineStat.index)):
		row=[lineStat.index[rowNum]] #Begins the row with date/time as label
		for stat in range(len(lineStat.iloc[rowNum])):
			row.append(lineStat.iloc[rowNum][stat]) #Add the arrival times
		csvWriter.writerow(row) #Writes the row to the file
	csvFileObj.close()

#Stations from Rosslyn to Stadium-Armory (Silver Orange Blue) to serve as input for lineStat
SOBLine=['C05', 'C04', 'C03', 'C02', 'C01', 'D01', 'D02', 'D03', 'D04', 'D05', 'D06', 'D07', 'D08']

