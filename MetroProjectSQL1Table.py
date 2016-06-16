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

def saveWMATASQL(trainData, engine): #saves the current WMATA data to open engine
	import datetime, pandas as pd
        #the line below creates a table name starting with WMATA and then containing the date and time information, with each day/hour/minute/second taking two characters
	DTstring=str(datetime.datetime.now().month)+str(datetime.datetime.now().day).rjust(2,'0')+str(datetime.datetime.now().hour).rjust(2,'0')+str(datetime.datetime.now().minute).rjust(2,'0')+str(datetime.datetime.now().second).rjust(2,'0')
	trainFrame=pd.DataFrame('-', index=range(len(trainData)), columns=['DT','Car','Loc','Lin','Des','Min','Gro']) #creates trainFrame, the DataFrame to send to the SQL server
	for iter in range(len(trainData)): #for all the trains in trainData
         trainFrame.loc[iter]['DT']=DTstring
         for colName in ['Car','LocationCode','Line','DestinationCode','Min','Group']: #select the six relevant fields
             trainFrame.loc[iter][colName[:3]]=trainData[iter][colName] #and fill in the relevant data  
	trainFrame.to_sql('WMATAFull', engine, if_exists='append') #send trainFrame to the SQL server
	return tableName

def WMATAtableSQL(timeMin,intervalSec): #records for timeMin minutes, about ever intervalSec seconds
	import time, pandas as pd
	from sqlalchemy import create_engine
	engine = create_engine('postgresql+psycopg2://Original:tolistbtGU!@teamoriginal.ccc95gjlnnnc.us-east-1.rds.amazonaws.com:5432/WmataData') #opens the engine to WmataData
	tableList=[] #creates a list of the table we're creating to add to the index 
	startTime=time.time()
	while time.time()<(startTime+60*timeMin): #runs for timeMin minutes
		stepStart=time.time()
		tableList.append(saveWMATASQL(JSONfromMetro(getMetroStatus()),engine)) #save the current train data and appends the name to tableList
		stepTime=time.time()-stepStart #calculates the time this step took
		time.sleep(intervalSec-stepTime) #wait a few seconds
	engine.connect().close()

def lineNextSQL(line, timeString,destList, engine): #reads the next train to arrive at the stations in line heading toward destList and returns it as a Data Frame
	import pandas as pd
	from sqlalchemy import create_engine
	if engine is None:  
		engine = create_engine('postgresql+psycopg2://Original:tolistbtGU!@teamoriginal.ccc95gjlnnnc.us-east-1.rds.amazonaws.com:5432/WmataData')
	rowName=pd.to_datetime('2016-'+timeString[0]+'-'+timeString[1:3]+' '+timeString[3:5]+':'+timeString[5:7]+':'+timeString[7:])
# names the row as a timestamp with the month        day                  hour                  minute             second
	lineStat=pd.DataFrame('-',index=[rowName],columns=line)
	for station in line: #repeat the below process for every station on the line
		query='SELECT * FROM "WMATAFull" WHERE "Loc"='+"'"+station+"'"+' AND "DT"='+"'"+timeString+"';"
		arrData=pd.read_sql(query,engine)
		trainNum=0 #the below lines look for the first train going to a destination in destList
		while trainNum<len(arrData.index) and (arrData.loc[trainNum]['Des'] not in destList):
			trainNum+=1
		if trainNum<len(arrData.index): #If you found a train
			lineStat.loc[rowName][station]=arrData.loc[trainNum]['Lin']+':'+arrData.loc[trainNum]['Min'] #set the station status to the color and ETA of arriving train
	if engine is None: 
		engine.connect().close()
	return lineStat

def lineNextTableSQL(line, firstTime,destList, iterNum): #saves the next train arrivals for a line and destList over time
	import pandas as pd
	from sqlalchemy import create_engine
	engine = create_engine('postgresql+psycopg2://Original:tolistbtGU!@teamoriginal.ccc95gjlnnnc.us-east-1.rds.amazonaws.com:5432/WmataData')
	lineStats=lineNextSQL(line,firstTime, destList, engine) #save the first stattus
	query='SELECT DISTINCT "DT" FROM "WMATAFull";'
	timesPD=pd.read_sql(query,engine)
	firstRow=list(timesPD.T.loc['DT']).index(firstTime)
	for num in range(1,min(iterNum,len(timesPD.index)-firstRow)): #run for iterNum or until the end
  		numTime=timesPD.iloc[firstRow+num][0]
  		lineStats=lineStats.append(lineNextSQL(line,numTime, destList, engine))
	engine.connect().close() 
	return lineStats

def trainBuild(lineStat,startTime): #determines how long it took the train arriving after startTime to reach every station and returns it as one row data frame
	import pandas as pd
	timeRow=list(lineStat.index).index(startTime) #finds the row number from lineStat labeled startTime and calls it timeRow
	specTrain=pd.DataFrame('0',index=[startTime],columns=['Col']+list(lineStat.columns)) #creates a one row DataFrame with the color and arrival times
	while timeRow<len(lineStat.index) and (len(lineStat.iloc[timeRow][0])<6 or lineStat.iloc[timeRow][0][-3:]!='BRD'): #while timeRow is in bounds and no train is boarding, 
          timeRow+=1 #go to the next line
	skipRows=timeRow-list(lineStat.index).index(startTime) #skipRows is the number of rows to skip the next time it looks for a train
	if timeRow>=len(lineStat.index): #if you get to the end,
             return [specTrain, skipRows] #just return what you have
	specTrain.loc[startTime]['Col']=lineStat.iloc[timeRow][0][:2] #fills in the color, which is stored as the first two letters in the status
	timeDif=lineStat.index[timeRow]-startTime #set timeDif to the diffence between arrival at this station and startTime
	specTrain.loc[startTime][lineStat.columns[0]]=str(timeDif.seconds) #store timeDif as seconds (converted into a string)
	for stationNum in range(1,len(lineStat.columns)): #this fills in the difference arrival time for every station
         while timeRow<len(lineStat.index) and lineStat.iloc[timeRow][stationNum]!=(specTrain.loc[startTime]['Col']+":BRD"): #while timeRow is in bounds and the train hasn't arrived, 
            timeRow+=1 #go to the next line
         if timeRow>=len(lineStat.index): #if you get to the end,
             return [specTrain, skipRows] #just return what you have
         timeDif=lineStat.index[timeRow]-startTime #set timeDif to the diffence between arrival at this station and startTime
         specTrain.loc[startTime][lineStat.columns[stationNum]]=str(timeDif.seconds) #store timeDif as seconds (converted into a string)
         timeRow+=2 #go down two rows, because the train will take at least 40-60 seconds to get to the next station
	return [specTrain, skipRows]

def trainTable(lineStat): #returns a table listing the trains by start time, color and the time they took to reach a given station
    import pandas as pd
    [masterTable,rowNum]=trainBuild(lineStat,lineStat.index[0]) #builds the first row and lets it now how many rows to go forward to get to the next train arrival
    currentColor=masterTable.iloc[0][0] #record the color of the first train as currentColor
    while rowNum<len(lineStat.index): #keep going as long as there's data to analyze
        while rowNum<len(lineStat.index) and lineStat.iloc[rowNum][0]==currentColor+':BRD': #while the train (with currentColor) is boarding,
            rowNum+=1 #go to the next row
        [newTrain, skipRows]=trainBuild(lineStat,lineStat.index[rowNum]) #once you've gotten to a new train arrival, record it as newTrain and note the rows to skip 
        masterTable=masterTable.append(newTrain) #append newTrain to the masterTable
        currentColor=masterTable.iloc[-1][0] #xchange currentColor to the color of the train that just boarded
        rowNum+=skipRows #skip ahead to the next train
    return masterTable

#Stations from Rosslyn to Stadium-Armory (Silver Orange Blue) to serve as input for LineStat
SOBLine=['C05', 'C04', 'C03', 'C02', 'C01', 'D01', 'D02', 'D03', 'D04', 'D05', 'D06', 'D07', 'D08']