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
	return trainFrame


def lineNextDF(line, destList, arrData):
	import pandas as pd
	timeString=arrData.DT.iloc[0]
	rowName=pd.to_datetime('2016-'+timeString[0]+'-'+timeString[1:3]+' '+timeString[3:5]+':'+timeString[5:7]+':'+timeString[7:])
# names the row as a timestamp with the month        day                  hour                  minute             second
	lineStat=pd.DataFrame('-',index=[rowName],columns=line)
	for station in line: #repeat the below process for every station on the line
		trains2consider=arrData.loc[lambda df: df.Loc==station].loc[lambda df: df.Des.isin(destList)] #pull out the trains at that station heading toward the destinations
		if len(trains2consider.index)>0: #If you found a train
			if trains2consider.Des.iloc[0] in ['A11','B08','E01','K04']: #the next few lines set the station status to the color and ETA of the first arriving train
        			lineStat.loc[rowName,station]=trains2consider.Lin.iloc[0].lower()+':'+trains2consider.Min.iloc[0]  #if the train is terminating early (at Grovesnor, Silver Spring or Mt Vernon), use lowercase
			elif trains2consider.Des.iloc[0]=='E06':
        			lineStat.loc[rowName,station]='Yl:'+trains2consider.Min.iloc[0]
			else:
        			lineStat.loc[rowName,station]=trains2consider.Lin.iloc[0]+':'+trains2consider.Min.iloc[0] #otherwise use upper
	return lineStat

def allLNtoNE(arrData, surgeNum): #all of the lines to the North and East during Surge 4
    import pandas as pd
    LNlist=[]
    for num in range(len(lineList[surgeNum])):
        LNlist.append(lineNextDF(lineList[surgeNum][num], NEdestList[surgeNum][num], arrData)) #run for each line and destination
    return pd.concat(LNlist, axis=1, join='outer') #then join them all together

def allLNtoSW(arrData, surgeNum): #all of the lines to the South and West during Surge 4
    import pandas as pd
    LNlist=[]
    for num in range(1,1+len(lineList[surgeNum])):
        LNlist.append(lineNextDF(lineList[surgeNum][-num][::-1], SWdestList[surgeNum][-num][::-1], arrData)) #run for each line and destination
    return pd.concat(LNlist, axis=1, join='outer') #then join them all together

def WMATAtableSQL(timeMin,intervalSec, surgeNum): #records for timeMin minutes, about ever intervalSec seconds
	import time, pandas as pd
	from sqlalchemy import create_engine
	engine = create_engine('postgresql+psycopg2://Original:tolistbtGU!@teamoriginal.ccc95gjlnnnc.us-east-1.rds.amazonaws.com:5432/WmataData') #opens the engine to WmataData
	 #creates a list of the table we're creating to add to the index 
	isStart=True
	startTime=time.time()
	while time.time()<(startTime+60*timeMin): #runs for timeMin minutes
         stepStart=time.time()
         WMATAdf=saveWMATASQL(JSONfromMetro(getMetroStatus()),engine) #save the current train data and appends the name to tableList
         if len(WMATAdf.index)>0: #if you got data back
             if isStart: #and it's the first row
                 allLN2NE=allLNtoNE(WMATAdf,surgeNum) #set allLNtoNE equal to the all LineNext to NE data
                 allLN2SW=allLNtoSW(WMATAdf,surgeNum) #set allLNtoSW equal to the all LineNext to SW data
                 isStart=False #and the next row will not be the first row
             else: #for other rows
                 allLN2NE=allLN2NE.append(allLNtoNE(WMATAdf,surgeNum)) #append the data
                 allLN2SW=allLN2SW.append(allLNtoSW(WMATAdf,surgeNum))
         stepTime=time.time()-stepStart #calculates the time this step took
         if stepTime<intervalSec: #if intervalSec seconds have not passed,
             time.sleep(intervalSec-stepTime) #wait until a total of intervalSec have passed
	engine.connect().close()
	return [allLN2NE, allLN2SW]

def lineNextSQL(line, timeString,destList, engine): #reads the next train to arrive at the stations in line heading toward destList and returns it as a Data Frame
	import pandas as pd
	from sqlalchemy import create_engine
	isEngineNone=(engine is None)
	if isEngineNone: #if there's not an engine, make one  
		engine = create_engine('postgresql+psycopg2://Original:tolistbtGU!@teamoriginal.ccc95gjlnnnc.us-east-1.rds.amazonaws.com:5432/WmataData')
	query='SELECT * FROM "WMATAFull" WHERE "DT"='+"'"+timeString+"';"
	arrData=pd.read_sql(query,engine)
	if isEngineNone: 
		engine.connect().close()
	return lineNextDF(line, destList, arrData)

def lineNextTableSQL(line, firstTime, lastTime, destList): #saves the next train arrivals for a line and destList over time
	import time, pandas as pd
	from sqlalchemy import create_engine
	print(time.strftime("%a, %d %b %Y %H:%M:%S"))
	engine = create_engine('postgresql+psycopg2://Original:tolistbtGU!@teamoriginal.ccc95gjlnnnc.us-east-1.rds.amazonaws.com:5432/WmataData')
	query='SELECT * FROM "WMATAFull" WHERE "DT">='+"'"+firstTime+"' AND "+'"DT"<='+"'"+lastTime+"';"
	arrData=pd.read_sql(query,engine)
	print(time.strftime("%a, %d %b %Y %H:%M:%S"))
	if len(arrData.index)==0:
         return None
	timesPD=arrData.DT.value_counts().sort_index().index #pull out each time and call it timesPD
	lineStats=lineNextDF(line, destList, arrData.loc[lambda df: df.DT==timesPD[0]]) #save the first status
	for num in range(1,len(timesPD)): #for each time
  		lineStats=lineStats.append(lineNextDF(line, destList, arrData.loc[lambda df: df.DT==timesPD[num]])) #add the data for that time
	engine.connect().close() 
	print(time.strftime("%a, %d %b %Y %H:%M:%S"))
	return lineStats

def allLNtoNEtable(firstTime, lastTime, surgeNum): #saves the next train arrivals for a line and destList over time
	import pandas as pd
	from sqlalchemy import create_engine
	engine = create_engine('postgresql+psycopg2://Original:tolistbtGU!@teamoriginal.ccc95gjlnnnc.us-east-1.rds.amazonaws.com:5432/WmataData')
	query='SELECT * FROM "WMATAFull" WHERE "DT">='+"'"+firstTime+"' AND "+'"DT"<='+"'"+lastTime+"';"
	arrData=pd.read_sql(query,engine)
	if len(arrData.index)==0: #if you didn't get any data,
         return None #return nothing
	timesPD=arrData.DT.value_counts().sort_index().index #pull out each time and call it timesPD
	lineStats=allLNtoNE(arrData.loc[lambda df: df.DT==timesPD[0]],surgeNum) #save the first status
	for num in range(1,len(timesPD)): #for each time
  		lineStats=lineStats.append(allLNtoNE(arrData.loc[lambda df: df.DT==timesPD[num]],surgeNum)) #add the data for that time
	engine.connect().close() 
	return lineStats

def allLNtoSWtable(firstTime, lastTime, surgeNum): #saves the next train arrivals for a line and destList over time
	import pandas as pd
	from sqlalchemy import create_engine
	engine = create_engine('postgresql+psycopg2://Original:tolistbtGU!@teamoriginal.ccc95gjlnnnc.us-east-1.rds.amazonaws.com:5432/WmataData')
	query='SELECT * FROM "WMATAFull" WHERE "DT">='+"'"+firstTime+"' AND "+'"DT"<='+"'"+lastTime+"';"
	arrData=pd.read_sql(query,engine)
	if len(arrData.index)==0: #if you didn't get any data,
         return None #return nothing
	timesPD=arrData.DT.value_counts().sort_index().index #pull out each time and call it timesPD
	lineStats=allLNtoSW(arrData.loc[lambda df: df.DT==timesPD[0]],surgeNum) #save the first status
	for num in range(1,len(timesPD)): #for each time
  		lineStats=lineStats.append(allLNtoSW(arrData.loc[lambda df: df.DT==timesPD[num]],surgeNum)) #add the data for that time
	engine.connect().close() 
	return lineStats

def trainBuild(lineStat,startTime): #determines how long it took the train arriving after startTime to reach every station and returns it as one row data frame
	import pandas as pd
	timeRow=list(lineStat.index).index(startTime) #finds the row number from lineStat labeled startTime and calls it timeRow
	specTrain=pd.concat([pd.DataFrame('-',index=[startTime],columns=['Col']),pd.DataFrame(0,index=[startTime],columns=list(lineStat.columns))], axis=1, join='outer')
	while timeRow<len(lineStat.index)-1 and (not isinstance(lineStat.iloc[timeRow][0], str) or len(lineStat.iloc[timeRow][0])<6 or lineStat.iloc[timeRow][0][-3:]!='BRD'): #while timeRow is in bounds and no train is boarding, 
          timeRow+=1 #go to the next line
	skipRows=timeRow-list(lineStat.index).index(startTime) #skipRows is the number of rows to skip the next time it looks for a train
	if timeRow>=len(lineStat.index): #if you get to the end,
             return [specTrain, skipRows] #just return what you have
	specTrain.loc[startTime,'Col']=lineStat.iloc[timeRow][0][:2] #fills in the color, which is stored as the first two letters in the status
	timeDif=lineStat.index[timeRow]-startTime #set timeDif to the diffence between arrival at this station and startTime
	specTrain.loc[startTime,lineStat.columns[0]]=timeDif.seconds #store timeDif as seconds 
	for stationNum in range(1,len(lineStat.columns)): #this fills in the difference arrival time for every station
         isTrainBoarding=False
         while timeRow<(len(lineStat.index)-1) and not isTrainBoarding: #while timeRow is in bounds and the train is not boarding
            #The line below says that a train is boarding if either it has status "BRD" or it has status "ARR" and 20 seconds later the station is waiting for a different train 
            isTrainBoarding=lineStat.iloc[timeRow][stationNum]==(specTrain.loc[startTime,'Col']+":BRD") or (lineStat.iloc[timeRow][stationNum]==(specTrain.loc[startTime,'Col']+":ARR") and (lineStat.iloc[timeRow+1][stationNum][:2]!=specTrain.loc[startTime,'Col']))
            timeRow+=1 #go to the next line
         if timeRow>=len(lineStat.index)-1: #if you get to the end,
             return [specTrain, skipRows] #just return what you have
         timeDif=lineStat.index[timeRow]-startTime #set timeDif to the diffence between arrival at this station and startTime
         specTrain.loc[startTime,lineStat.columns[stationNum]]=timeDif.seconds #store timeDif as seconds (converted into a string)
         if stationNum<len(lineStat.columns)-1: #if you found a trains, go down a certain number of rows before checking the next station
             if lineStat.columns[stationNum] in minDist.keys() and lineStat.columns[stationNum+1] in minDist.keys(): #if both stations are in minDist
                 timeRow+=minDist[lineStat.columns[stationNum]][lineStat.columns[stationNum+1]]['weight'] #go down the number of rows recorded in minDist
             else:
                 timeRow+=2 #if the connection isn't in minDist, go down two rows
         if (specTrain.loc[startTime,'Col'].islower() and lineStat.columns[stationNum] in ['A11','B08','E01','K04']) or (specTrain.loc[startTime,'Col']=='Yl' and lineStat.columns[stationNum]=='E05'):
             break
	return [specTrain, skipRows]

def trainTable(lineStat): #returns a table listing the trains by start time, color and the time they took to reach a given station
    import pandas as pd
    [masterTable,rowNum]=trainBuild(lineStat,lineStat.index[0]) #builds the first row and lets it now how many rows to go forward to get to the next train arrival
    currentColor=masterTable.iloc[0][0] #record the color of the first train as currentColor
    newTrain=masterTable #newTrain just needs to be something for when it's referenced in the if statement
    while rowNum<len(lineStat.index):# and newTrain.iloc[0][-1]!=0: #keep going as long as there's data to analyze and each train gets to the end
        while rowNum<len(lineStat.index)-1 and lineStat.iloc[rowNum][0]==currentColor+':BRD': #while the train (with currentColor) is boarding,
            rowNum+=1 #go to the next row
        [newTrain, skipRows]=trainBuild(lineStat,lineStat.index[rowNum]) #once you've gotten to a new train arrival, record it as newTrain and note the rows to skip 
        masterTable=masterTable.append(newTrain) #append newTrain to the masterTable
        currentColor=masterTable.iloc[-1][0] #xchange currentColor to the color of the train that just boarded
        rowNum+=skipRows+1 #skip ahead to the next train
    return masterTable

def lastBRDtime(newTrainBRDtime, lineStat, stationNum): #finds the last time a train boarded at a given station before newTrainBRDtime
    import pandas as pd
    timeRow=list(lineStat.index).index(newTrainBRDtime)-2 #start with a time two rows before the train reaches the station
    isTrainBoarding=False # the next few lines just say keep moving backwards in time until you get to a train board
    while timeRow>0 and not isTrainBoarding: #if you haven't hit the beginning and a train isn't boarding
        isTrainBoarding=isinstance(lineStat.iloc[timeRow,stationNum], str) and len(lineStat.iloc[timeRow, stationNum])==6 and lineStat.iloc[timeRow,stationNum][-3:]=='BRD' #a train is boarding if it's a string of length 6 with BRD as the last three letters
        timeRow-=1
    return lineStat.index[timeRow] #return that time

def trainTableIntermediate(lineStat, stationList): #returns a table listing the trains by start time, color and the time they took to reach a given station, with the possibility that a train started at an intermediary station
    import pandas as pd
    staNumList=[]
    for station in stationList: #turn the list of stations into a list of numbers corresponding to the stations' location in lineStat's columns
        staNumList.append(list(lineStat.columns).index(station))
    [masterTable,rowNum]=trainBuild(lineStat,lineStat.index[0]) #builds the first row and lets it now how many rows to go forward to get to the next train arrival
    currentColor=masterTable.iloc[0][0] #record the color of the first train as currentColor
    newTrain=masterTable #newTrain just needs to be something for when it's referenced in the if statement
    while rowNum<len(lineStat.index):# and newTrain.iloc[0][-1]!=0: #keep going as long as there's data to analyze and each train gets to the end
        while rowNum<len(lineStat.index)-1 and lineStat.iloc[rowNum,0]==currentColor+':BRD': #while the train (with currentColor) is boarding,
            rowNum+=1 #go to the next row
        [newTrain, skipRows]=trainBuild(lineStat,lineStat.index[rowNum]) #once you've gotten to a new train arrival, record it as newTrain and note the rows to skip
        for staNum in staNumList: #for all the intermediary stations in stationList
            mostRecentBRDtime=lastBRDtime(newTrain.index[0]+pd.to_timedelta(newTrain.iloc[0,staNum],unit='s'), lineStat, staNum) #find the last train to board at this station
            if mostRecentBRDtime>=masterTable.index[-1]+pd.to_timedelta(masterTable.iloc[-1,staNum]+42,unit='s'): #if that train left more than 42 seconds after the last train in the table
                intermedTrain=trainBuild(lineStat.iloc[:,staNum:],mostRecentBRDtime)[0] #it's a different train, so figure out how long it took to arrive
                for colNum in range(staNum): #for all the stations before the intermediary station,
                    intermedTrain.insert(colNum+1,lineStat.columns[colNum],0)  #insert a column with value 0 and the correct station
                masterTable=masterTable.append(intermedTrain) #append the intermediary train to masterTable
                break
        masterTable=masterTable.append(newTrain) #append newTrain to the masterTable
        currentColor=newTrain.iloc[0,0] #change currentColor to the color of the train that just boarded
        rowNum+=skipRows+1 #skip ahead to the next train
    masterTable.index+=pd.to_timedelta(masterTable.iloc[:,staNum],unit='s') #normalize the index so that the time in each row is when the train arrived at the first station to include all trains, i.e., the last intermediary station
    return masterTable
    
    
def trainTableMerge(innerTrainTable, outerTrainTable): #this function merges two sets of train tables, where all the stations in innerTrainTable are also in outerTrainTable
    import pandas as pd
    centralTrainTable=innerTrainTable.loc[lambda df: df.Col>'ZZ'] #only worry about the trains that are lowercase
    numLeftStations=list(outerTrainTable.columns).index(centralTrainTable.columns[1]) #numLeftStations is the number of stations in outerTrainTable before the first one in innerTrainTable
    numRightStations=len(outerTrainTable.columns)-list(outerTrainTable.columns).index(centralTrainTable.columns[-1])-1 #numRightStations is the number of stations in outerTrainTable after the last one in innerTrainTable
    if numLeftStations>1:
        for staNum in range(numLeftStations-1): #insert the left stations from outerTrainTable with 0 as the value
            centralTrainTable.insert(1+staNum,outerTrainTable.columns[1+staNum],0)
    if numRightStations>0:
        for staNum in range(-numRightStations,0): #insert the right stations from outerTrainTable with 0 as the value
            centralTrainTable.insert(len(centralTrainTable.columns),outerTrainTable.columns[staNum],0)
    newTrainTable=pd.concat([outerTrainTable,centralTrainTable]) #join the two tables together
    newTrainTable.index+=pd.to_timedelta(newTrainTable.iloc[:,numLeftStations],unit='s') #normalize the index so that the time in each row is when the train arrived at the first station to include all trains, i.e., the last intermediary station
    return newTrainTable.sort_index() #return the combined table sorted by when the trains arrived at the first station in innerTrainTable

def allTrainsNE(allLN2NE, surgeNum): #returns all trains heading toward the North and East (Glenmont, Greenbelt, New Carrollton, Largo) as a dictioanry of panda dataframes
    GRtrains=trainTable(allLN2NE.loc[:, sGLine+cGYLine+nGYEnd]) #produces all green line trains
    if surgeNum in [3,4]: #if it's surge 3 or 4
        YLtrains=trainTable(allLN2NE.loc[:, ['C07']+cGYLine+nGYEnd]).loc[lambda df: df.Col.isin(['YL','yl'])] #return yellow line trains starting at the Pentagon 
        BLtrains=trainTable(allLN2NE.loc[:, ['C07']+BArlCem+SOBLine+SBLine]) #return blue line trains starting at the Pentagon 
    else:
        YLtrains=trainTable(allLN2NE.loc[:,wBEnd+BYLine+cGYLine+nGYEnd]).loc[lambda df: df.Col=='YL'] #otherwise, return yellow line Rush Plus trains from Van Dorn onward
        yltrains=trainTable(allLN2NE.loc[:, sYEnd+BYLine+cGYLine+nGYEnd[:6]]).loc[lambda df: df.Col.isin(['Yl','yl'])] #return normal yellow line trains 
        if surgeNum==2:
            BLtrains=trainTable(allLN2NE.loc[:, wBEnd+BYLine]).loc[lambda df: df.Col=='BL'] #for surge 2, return trains that run to the Pentagon
        else:        
            BLtrains=trainTable(allLN2NE.loc[:, wBEnd+BYLine+BArlCem+SOBLine+SBLine]).loc[lambda df: df.Col=='BL'] #otherwise, blue lines run all the way
    if surgeNum==2:
        SVtrains=trainTable(allLN2NE.loc[:, wSEnd+SOLine+SOBLine[:-3]]) #for surge 2, return trains that run to Eastern Market
        ORtrains=trainTable(allLN2NE.loc[:, wOEnd+SOLine+SOBLine[:-3]]) #for surge 2, return trains that run to Eastern Market
    else:
        SVtrains=trainTable(allLN2NE.loc[:, wSEnd+SOLine+SOBLine+SBLine]) #otherwise, silver line trains run all the way
        if surgeNum in [1,5]: #during surges 1 and 5, there are intermediate orange line trains
            ORtrains=trainTableIntermediate(allLN2NE.loc[:, wOEnd+SOLine+SOBLine+eOLine], ['K04']) #that start at Ballston (K04)
        else:
            ORtrains=trainTable(allLN2NE.loc[:, wOEnd+SOLine+SOBLine[:-1]])
    RDtrains=trainTableMerge(trainTable(allLN2NE.loc[:, cRedLine]),trainTable(allLN2NE.loc[:, wRedEnd+cRedLine+eRedEnd])) #for red line trains, produce trains that run all the way and trains that run from Grovesnor to Silver Spring
    if surgeNum in [3,4]:
        return {'GR':GRtrains,'YL':YLtrains,'BL':BLtrains,'SV':SVtrains,'OR':ORtrains,'RD':RDtrains} #combine them all into a dictionary
    else:
        return {'GR':GRtrains,'YL':YLtrains,'yl':yltrains,'BL':BLtrains,'SV':SVtrains,'OR':ORtrains,'RD':RDtrains} #combine them all into a dictionary

#note: [::-1] reverses the direction of a list without affecting how it's called later
def allTrainsSW(allLN2SW,surgeNum):  #returns all trains heading toward the South and West (Branch Ave, Huntington, Franconia-Springfield, Vienna, Wiehle, Shady Grove) as a dictionary of panda dataframes
    GRtrains=trainTable(allLN2SW.loc[:, (sGLine+cGYLine+nGYEnd)[::-1]]).loc[lambda df: df.Col=='GR'] #produces all green line trains
    if surgeNum in [3,4]: #if it's surge 3 or 4
        YLtrains=trainTableIntermediate(allLN2SW.loc[:, (['C07']+cGYLine+nGYEnd)[::-1]],['E06','E01']).loc[lambda df: df.Col.isin(['YL','Yl','yl'])] #run the yellow line to the Pentagon
        BLtrains=trainTable(allLN2SW.loc[:, (['C07']+BArlCem+SOBLine+SBLine)[::-1]]).loc[lambda df: df.Col=='BL'] #run the blue line to the Pentagon
    else:
        YLtrains=trainTable(allLN2SW.loc[:, (wBEnd+BYLine+cGYLine+nGYEnd)[::-1]]).loc[lambda df: df.Col=='YL'] #otherwise, produce yellow line Rush Plus trains to Franconia-Springfied 
        yltrains=trainTableIntermediate(allLN2SW.loc[:, (sYEnd+BYLine+cGYLine+nGYEnd[:6])[::-1]],['E06','E01']).loc[lambda df: df.Col.isin(['yl','YL'])] #return normal yellow line trains
        if surgeNum==2:
            BLtrains=trainTable(allLN2SW.loc[:, (wBEnd+BYLine)[::-1]])[lambda df: df.Col=='BL']    #for surge 2, produce trains from the Pentagon to Van Dorn
        else:
            BLtrains=trainTable(allLN2SW.loc[:, (wBEnd+BYLine+BArlCem+SOBLine+SBLine)[::-1]]).loc[lambda df: df.Col=='BL']
    if surgeNum==2:
        SVtrains=trainTable(allLN2SW.loc[:, (wSEnd+SOLine+SOBLine[:-3])[::-1]]).loc[lambda df: df.Col=='SV'] #for surge 2, produce trains that run to Eastern Market
        ORtrains=trainTable(allLN2SW.loc[:, (wOEnd+SOLine+SOBLine[:-3])[::-1]]).loc[lambda df: df.Col=='OR'] #for surge 2, produce trains that run to Eastern Market
    else:
        SVtrains=trainTable(allLN2SW.loc[:, (wSEnd+SOLine+SOBLine+SBLine)[::-1]]).loc[lambda df: df.Col=='SV'] #produce silver line trains
        ORtrains=trainTable(allLN2SW.loc[:, (wOEnd+SOLine+SOBLine+eOLine)[::-1]]) #produce orange line trains
    RDtrains=trainTableMerge(trainTable(allLN2SW.loc[:, cRedLine[::-1]]),trainTable(allLN2SW.loc[:, (wRedEnd+cRedLine+eRedEnd)[::-1]])) #for red line trains, return trains that run all the way and trains that run from Grovesnor to Silver Spring
    if surgeNum in [3,4]:
        return {'GR':GRtrains,'YL':YLtrains,'BL':BLtrains,'SV':SVtrains,'OR':ORtrains,'RD':RDtrains} #combine them all into a dictionary
    else:
        return {'GR':GRtrains,'YL':YLtrains,'yl':yltrains,'BL':BLtrains,'SV':SVtrains,'OR':ORtrains,'RD':RDtrains} #combine them all into a dictionary


def trainTableErrHandling(lineStat): #returns a table listing the trains by start time, color and the time they took to reach a given station, removing trains that arrive at the same time or before the train that started before them
    import pandas as pd
    [masterTable,rowNum]=trainBuild(lineStat,lineStat.index[0]) #builds the first row and lets it now how many rows to go forward to get to the next train arrival
    currentColor=masterTable.iloc[0][0] #record the color of the first train as currentColor
    newTrain=masterTable #newTrain just needs to be something for when it's referenced in the if statement
    while rowNum<len(lineStat.index) and newTrain.iloc[0][-1]!=0: #keep going as long as there's data to analyze and each train gets to the end
        while rowNum<len(lineStat.index)-1 and lineStat.iloc[rowNum][0]==currentColor+':BRD': #while the train (with currentColor) is boarding,
            rowNum+=1 #go to the next row
        [newTrain, skipRows]=trainBuild(lineStat,lineStat.index[rowNum]) #once you've gotten to a new train arrival, record it as newTrain and note the rows to skip 
        if newTrain.iloc[0][-1]==0 or (newTrain.index[0]-masterTable.index[-1]).seconds>int(masterTable.iloc[-1][-1])-int(newTrain.iloc[0][-1]): #if you've reached the end or newTrain arrived at the last station after the last train
            masterTable=masterTable.append(newTrain) #append newTrain to the masterTable
        else: #but if that's not the case, something went wrong with the last train
            masterTable=pd.concat([masterTable.iloc[:][:-1],newTrain]) #replace the last row of masterTable with the data for newTrain
        currentColor=masterTable.iloc[-1][0] #exchange currentColor to the color of the train that just boarded
        rowNum+=skipRows+1 #skip ahead to the next train
    return masterTable

def trainTableSurgeNE(month, dayList, surgeNum): #this code asssembles all the trains moving toward the North and East during a surge
    import pandas as pd
    isFirst=True
    for day in dayList: #for all the days on the list
        tempLN=allLNtoNEtable(str(month)+str(day).rjust(2,'0')+'045000',str(month)+str(day).rjust(2,'0')+'101000',surgeNum) #form the lineNext for the morning
        if isinstance(tempLN, pd.DataFrame) and len(tempLN.index)>200: #if there's more than 200 lines (over an hour) of data
            if isFirst: #if it's the first time you found data
                trainsSurge=allTrainsNE(tempLN,surgeNum) #set the data to the trains of that set
                isFirst=False
            else: #if it's not the first time you found data
                tempTrains=allTrainsNE(tempLN,surgeNum)
                for color in trainsSurge.keys(): #for each color
                    trainsSurge[color]=trainsSurge[color].append(tempTrains[color]) #append the new data to the existing data
        #this is the same as above, but for the afternoon instead of the morning
        tempLN=allLNtoNEtable(str(month)+str(day).rjust(2,'0')+'165000',str(month)+str(day)+'221000',surgeNum)
        if isinstance(tempLN, pd.DataFrame) and len(tempLN.index)>200:
            if isFirst:
                trainsSurge=allTrainsNE(tempLN,surgeNum)
                isFirst=False
            else:
                tempTrains=allTrainsNE(tempLN,surgeNum)
                for color in trainsSurge.keys():
                    trainsSurge[color]=trainsSurge[color].append(tempTrains[color])
    return trainsSurge                    

#this is the same as above, but for trains heading to the South and West instead
def trainTableSurgeSW(month, dayList, surgeNum):
    import pandas as pd
    isFirst=True
    for day in dayList:
        tempLN=allLNtoSWtable(str(month)+str(day).rjust(2,'0')+'045000',str(month)+str(day).rjust(2,'0')+'101000',surgeNum)
        if isinstance(tempLN, pd.DataFrame) and len(tempLN.index)>200:
            if isFirst:
                trainsSurge=allTrainsSW(tempLN,surgeNum)
                isFirst=False
            else:
                tempTrains=allTrainsSW(tempLN,surgeNum)
                for color in trainsSurge.keys():
                    trainsSurge[color]=trainsSurge[color].append(tempTrains[color])
        tempLN=allLNtoSWtable(str(month)+str(day).rjust(2,'0')+'165000',str(month)+str(day).rjust(2,'0')+'221000',surgeNum)
        if isinstance(tempLN, pd.DataFrame) and len(tempLN.index)>200:
            if isFirst:
                trainsSurge=allTrainsSW(tempLN,surgeNum)
                isFirst=False
            else:
                tempTrains=allTrainsSW(tempLN,surgeNum)
                for color in trainsSurge.keys():
                    trainsSurge[color]=trainsSurge[color].append(tempTrains[color])
    return trainsSurge
 
def saveWMATAtrainSQL(timeList, duration, surgeNum): #saves the data from WMATA and the train instances to the SQL database
    import time, pandas as pd
    from sqlalchemy import create_engine
    for hour2wake in timeList: #run at timeList
        while  int(time.strftime('%H'))<hour2wake or int(time.strftime('%H'))>timeList[-1]: #while it's not yet time to run
               time.sleep(180) #wait 3 minutes
        [allLN2NE,allLN2SW]=WMATAtableSQL(duration,20,5) #then run for duration period, recording data every 20 seconds
        allLN2NE.to_csv('LNtoNE'+time.strftime("%d%H%M%S")+'.csv') #this saves a copy of the lineNext, so I can reconstruct the trainBuild if there's a problem
        allLN2SW.to_csv('LNtoSW'+time.strftime("%d%H%M%S")+'.csv') #ditto
        trains2NE=allTrainsNE(allLN2NE, surgeNum) #this creates the train dictionary
        trains2SW=allTrainsNE(allLN2SW, surgeNum) #ditto
        engine = create_engine('postgresql+psycopg2://Original:tolistbtGU!@teamoriginal.ccc95gjlnnnc.us-east-1.rds.amazonaws.com:5432/WmataData') #opens the engine to WmataData
        for color in trains2NE.keys(): #for all the colors
            if color=='YL': #for rush plus yellow lines, save them as YLP and the surge number
                trains2NE['YL'].to_sql('NEtrainsYLP'+str(surgeNum), engine, if_exists='append')
                trains2SW['YL'].to_sql('SWtrainsYLP', engine, if_exists='append')
            else: #for other colors, save them by the color and the surge number
                trains2NE[color].to_sql('NEtrains'+color+str(surgeNum), engine, if_exists='append')
                trains2SW[color].to_sql('SWtrains'+color+str(surgeNum), engine, if_exists='append')
        engine.connect().close()
    return
    
#these are the lines to examine. lowercase letters (w,e,n,s,c) are the ordinal directions and central. Uppercase letters (O,S,B,Y,G) and Red are the lines
wOEnd=['K07','K06']
wSEnd=['N04','N03','N02','N01']
SOLine=['K05', 'K04','K03','K02','K01']
wBEnd=['J02']
sYEnd=['C14']
BYLine=['C13','C12','C10','C09','C08','C07']
BArlCem=['C06']
SOBLine=['C05', 'C04', 'C03', 'C02', 'C01', 'D01', 'D02', 'D03', 'D04', 'D05', 'D06', 'D07', 'D08']
SBLine=['G01','G02','G03','G04']
eOLine=['D09','D10','D11','D12']
sGLine=['F10','F09', 'F08', 'F07', 'F06', 'F05', 'F04']
cGYLine=['F03', 'F02', 'F01']
nGYEnd=['E01', 'E02', 'E03', 'E04', 'E05', 'E06', 'E07', 'E08', 'E09']
wRedEnd=['A14','A13', 'A12', 'A11']
cRedLine=['A10', 'A09', 'A08', 'A07', 'A06', 'A05', 'A04', 'A03', 'A02', 'A01', 'B01', 'B02', 'B03', 'B35', 'B04', 'B05', 'B06', 'B07']
eRedEnd=['B08','B09','B10']

#dictionaries to keep track of the Metro during SafeTrack surges
#these are the lines to examine. lowercase letters (w,e,n,s,c) are the ordinal directions and central. Uppercase letters are the lines
lineList={0:[wOEnd, wSEnd, SOLine, wBEnd, sYEnd, BYLine, BArlCem, SOBLine, eOLine, SBLine, sGLine, cGYLine, nGYEnd, wRedEnd, cRedLine, eRedEnd],
1:[wOEnd, wSEnd, SOLine[:2], SOLine[2:], wBEnd, sYEnd, BYLine, BArlCem, SOBLine, eOLine, SBLine, sGLine, cGYLine, nGYEnd, wRedEnd, cRedLine, eRedEnd],
2:[wOEnd, wSEnd, SOLine, wBEnd, sYEnd, BYLine, SOBLine[:-3], sGLine, cGYLine, nGYEnd, wRedEnd, cRedLine, eRedEnd],
3:[wOEnd, wSEnd, SOLine, BYLine[:3], BArlCem, SOBLine, eOLine, SBLine, sGLine, cGYLine, nGYEnd, wRedEnd, cRedLine, eRedEnd],
4:[wOEnd, wSEnd, SOLine, ['C07'], BArlCem, SOBLine, eOLine, SBLine, sGLine, cGYLine, nGYEnd, wRedEnd, cRedLine, eRedEnd],
5:[wOEnd, wSEnd, SOLine[:2], SOLine[2:], wBEnd, sYEnd, BYLine, BArlCem, SOBLine, eOLine, SBLine, sGLine, cGYLine, nGYEnd, wRedEnd, cRedLine, eRedEnd],
6:[wOEnd, wSEnd, SOLine, wBEnd, sYEnd, BYLine, BArlCem, SOBLine, eOLine, SBLine, sGLine, cGYLine, nGYEnd, wRedEnd, cRedLine[:-5], cRedLine[-5:]+eRedEnd]}

#these are the destinations along those lines. D13=New Car... G05=Largo E01=Mt Vernon E10=Greenbelt B08=Silver Spring B11=Glenmont    
NEdestList={0:[['D13'],['G05'],['D13','G05'],['G05','E10'],['E01','E06','E10'],['G05','E01','E06','E10'],['G05'],['G05','D13','G05'],['D13'],['G05'],['E10'],['E01','E06','E10'],['E06','E10'],['B11'],['B08','B11'],['B11']],
1:[['D13'],['G05'],['D13','G05'],['D13','G05'],['G05','E10'],['E01','E06','E10'],['G05','E01','E06','E10'],['G05'],['G05','D13','G05'],['D13'],['G05'],['E10'],['E01','E06','E10'],['E06','E10'],['B11'],['B08','B11'],['B11']],
2:[['D06'],['D06'],['D06'],['C06','E10'],['E01','E06','E10'],['C06','E01','E06','E10'],['D06'],['E10'],['E01','E06','E10'],['E06','E10'],['B11'],['B08','B11'],['B11']],
3:[['D13'],['G05'],['D13','G05'],['G05','E01','E06','E10'],['G05'],['G05','D13','G05'],['D13'],['G05'],['E10'],['E01','E06','E10'],['E10'],['B11'],['B08','B11'],['B11']],
4:[['D13'],['G05'],['D13','G05'],['G05','E01','E06','E10'],['G05'],['G05','D13','G05'],['D13'],['G05'],['E10'],['E01','E06','E10'],['E10'],['B11'],['B08','B11'],['B11']],
5:[['D13'],['G05'],['D13','G05'],['D13','G05'],['G05','E10'],['E01','E06','E10'],['G05','E01','E06','E10'],['G05'],['G05','D13','G05'],['D13'],['G05'],['E10'],['E01','E06','E10'],['E06','E10'],['B11'],['B08','B11'],['B11']],
6:[['D13'],['G05'],['D13','G05'],['G05','E10'],['E01','E06','E10'],['G05','E01','E06','E10'],['G05'],['G05','D13','G05'],['D13'],['G05'],['E10'],['E01','E06','E10'],['E06','E10'],['B11'],['B35','B11'],['B11']]}

#these are the destinations along those lines. K08=Vienna N06=Wiehle C08=Pentagon City F11=Branch Ave A15=Shady Gr A11=Grovesnor
SWdestList={0:[['K08'],['N06'],['K08','N06'],['J03'],['C15'],['C15','J03'],['J03'],['K08','J03','N06'],['K08'],['J03','N06'],['F11'],['F11', 'C15'],['F11', 'C15'],['A15'],['A11','A15'],['A15']],
1:[['K08'],['N06'],['K08','N06'],['K08','K04','N06'],['J03'],['C15'],['C15','J03'],['J03'],['K08','K04','J03','N06'],['K08','K04'],['J03','K04','N06'],['F11'],['F11', 'C15'],['F11', 'C15'],['A15'],['A11','A15'],['A15']],
2:[['K08'],['N06'],['K08','N06'],['J03'],['C15'],['C15','J03'],['K08','N06'],['F11'],['F11', 'C15'],['F11', 'C15'],['A15'],['A11','A15'],['A15']],
3:[['K08'],['N06'],['K08','N06'],['C10'],['C10'],['K08','C10','N06'],['K08'],['C10','N06'],['F11'],['F11', 'C10'],['F11', 'C10'],['A15'],['A11','A15'],['A15']],
4:[['K08'],['N06'],['K08','N06'],['C08'],['C08'],['K08','C08','N06'],['K08'],['C08','N06'],['F11'],['F11', 'C08'],['F11', 'C08'],['A15'],['A11','A15'],['A15']],
5:[['K08'],['N06'],['K08','N06'],['K08','K04','N06'],['J03'],['C15'],['C15','J03'],['J03'],['K08','K04','J03','N06'],['K08','K04'],['J03','K04','N06'],['F11'],['F11', 'C15'],['F11', 'C15'],['A15'],['A11','A15'],['A15']]}

#minDist is used by trainBuild to determine the minimum time between stations, in case there are two trains on the same line boarding at adjacent stations
minDist={'A01': {'A02': {'weight': 4}, 'B01': {'weight': 2}},
 'A02': {'A01': {'weight': 4}, 'A03': {'weight': 2}},
 'A03': {'A02': {'weight': 2}, 'A04': {'weight': 4}},
 'A04': {'A03': {'weight': 4}, 'A05': {'weight': 3}},
 'A05': {'A04': {'weight': 3}, 'A06': {'weight': 2}},
 'A06': {'A05': {'weight': 2}, 'A07': {'weight': 5}},
 'A07': {'A06': {'weight': 5}, 'A08': {'weight': 3}},
 'A08': {'A07': {'weight': 3}, 'A09': {'weight': 5}},
 'A09': {'A08': {'weight': 5}, 'A10': {'weight': 4}},
 'A10': {'A09': {'weight': 4}, 'A11': {'weight': 6}},
 'A11': {'A10': {'weight': 6}, 'A12': {'weight': 4}},
 'A12': {'A11': {'weight': 4}, 'A13': {'weight': 4}},
 'A13': {'A12': {'weight': 4}, 'A14': {'weight': 5}},
 'A14': {'A13': {'weight': 5}},
 'B01': {'A01': {'weight': 2}, 'B02': {'weight': 2}},
 'B02': {'B01': {'weight': 2}, 'B03': {'weight': 4}},
 'B03': {'B02': {'weight': 4}, 'B35': {'weight': 3}},
 'B04': {'B05': {'weight': 3}, 'B35': {'weight': 4}},
 'B05': {'B04': {'weight': 3}, 'B06': {'weight': 4}},
 'B06': {'B05': {'weight': 4}, 'B07': {'weight': 5}},
 'B07': {'B06': {'weight': 5}, 'B08': {'weight': 2}},
 'B08': {'B07': {'weight': 2}, 'B09': {'weight': 6}},
 'B09': {'B08': {'weight': 6}, 'B10': {'weight': 6}},
 'B10': {'B09': {'weight': 6}},
 'B35': {'B03': {'weight': 3}, 'B04': {'weight': 4}},
 'C01': {'C02': {'weight': 2}, 'D01': {'weight': 2}},
 'C02': {'C01': {'weight': 2}, 'C03': {'weight': 2}},
 'C03': {'C02': {'weight': 2}, 'C04': {'weight': 3}},
 'C04': {'C03': {'weight': 3}, 'C05': {'weight': 5}},
 'C05': {'C04': {'weight': 5}, 'C06': {'weight': 4}, 'K01': {'weight': 2}},
 'C06': {'C05': {'weight': 4}, 'C07': {'weight': 5}},
 'C07': {'C06': {'weight': 5}, 'C08': {'weight': 3}, 'F03': {'weight': 9}},
 'C08': {'C07': {'weight': 3}, 'C09': {'weight': 3}},
 'C09': {'C08': {'weight': 3}, 'C10': {'weight': 3}},
 'C10': {'C09': {'weight': 3}, 'C12': {'weight': 8}},
 'C12': {'C10': {'weight': 8}, 'C13': {'weight': 4}},
 'C13': {'C12': {'weight': 4}, 'J02': {'weight': 9}},
 'D01': {'C01': {'weight': 2}, 'D02': {'weight': 2}},
 'D02': {'D01': {'weight': 2}, 'D03': {'weight': 4}},
 'D03': {'D02': {'weight': 4}, 'D04': {'weight': 2}},
 'D04': {'D03': {'weight': 2}, 'D05': {'weight': 3}},
 'D05': {'D04': {'weight': 3}, 'D06': {'weight': 2}},
 'D06': {'D05': {'weight': 2}, 'D07': {'weight': 2}},
 'D07': {'D06': {'weight': 2}, 'D08': {'weight': 4}},
 'D08': {'D07': {'weight': 4}, 'D09': {'weight': 6},'G01': {'weight': 8}},
 'D09': {'D08': {'weight': 6}, 'D10': {'weight': 4}},
 'D10': {'D09': {'weight': 4}, 'D11': {'weight': 4}},
 'D11': {'D10': {'weight': 4}, 'D12': {'weight': 4}},
 'D12': {'D11': {'weight': 4}},
 'E01': {'E02': {'weight': 2}, 'F01': {'weight': 2}},
 'E02': {'E01': {'weight': 2}, 'E03': {'weight': 2}},
 'E03': {'E02': {'weight': 2}, 'E04': {'weight': 3}},
 'E04': {'E03': {'weight': 3}, 'E05': {'weight': 3}},
 'E05': {'E04': {'weight': 3}, 'E06': {'weight': 5}},
 'E06': {'E05': {'weight': 5}, 'E07': {'weight': 5}},
 'E07': {'E06': {'weight': 5}, 'E08': {'weight': 3}},
 'E08': {'E07': {'weight': 3}, 'E09': {'weight': 2}},
 'E09': {'E08': {'weight': 2}},
 'F01': {'E01': {'weight': 2}, 'F02': {'weight': 2}},
 'F02': {'F01': {'weight': 2}, 'F03': {'weight': 2}},
 'F03': {'C07': {'weight': 9}, 'F02': {'weight': 2}, 'F04': {'weight': 4}},
 'F04': {'F03': {'weight': 4}, 'F05': {'weight': 3}},
 'F05': {'F04': {'weight': 3}, 'F06': {'weight': 4}},
 'F06': {'F05': {'weight': 4}, 'F07': {'weight': 4}},
 'F07': {'F06': {'weight': 4}, 'F08': {'weight': 4}},
 'F08': {'F07': {'weight': 4}, 'F09': {'weight': 3}},
 'F09': {'F08': {'weight': 3}, 'F10': {'weight': 5}},
 'F10': {'F09': {'weight': 5}},
 'G01': {'D08': {'weight': 8}, 'G02': {'weight': 5}},
 'G02': {'G01': {'weight': 5}, 'G03': {'weight': 4}},
 'G03': {'G02': {'weight': 4}, 'G04': {'weight': 5}},
 'G04': {'G03': {'weight': 5}},
 'J02': {'C13': {'weight': 9}},
 'K01': {'C05': {'weight': 2}, 'K02': {'weight': 2}},
 'K02': {'K01': {'weight': 2}, 'K03': {'weight': 2}},
 'K03': {'K02': {'weight': 2}, 'K04': {'weight': 2}},
 'K04': {'K03': {'weight': 2}, 'K05': {'weight': 2}},
 'K05': {'K04': {'weight': 2}, 'K06': {'weight': 2}, 'N01': {'weight': 2}},
 'K06': {'K05': {'weight': 2}, 'K07': {'weight': 2}},
 'K07': {'K06': {'weight': 2}},
 'N01': {'K05': {'weight': 2}, 'N02': {'weight': 2}},
 'N02': {'N01': {'weight': 2}, 'N03': {'weight': 2}},
 'N03': {'N02': {'weight': 2}, 'N04': {'weight': 2}},
 'N04': {'N03': {'weight': 2}}}