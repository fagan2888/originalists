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

def WMATAtableSQL(timeMin,intervalSec): #records for timeMin minutes, about ever intervalSec seconds
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
                 allLNtoNE=allLNtoNE4(WMATAdf) #set allLNtoNE equal to the all LineNext to NE data
                 allLNtoSW=allLNtoSW4(WMATAdf) #set allLNtoSW equal to the all LineNext to SW data
                 isStart=False #and the next row will not be the first row
             else: #for other rows
                 allLNtoNE=allLNtoNE.append(allLNtoNE4(WMATAdf)) #append the data
                 allLNtoSW=allLNtoSW.append(allLNtoSW4(WMATAdf))
         stepTime=time.time()-stepStart #calculates the time this step took
         if stepTime<intervalSec: #if intervalSec seconds have not passed,
             time.sleep(intervalSec-stepTime) #wait until a total of intervalSec have passed
	engine.connect().close()
	return [allLNtoNE, allLNtoSW]

def lineNextDF(line, destList, arrData):
	import pandas as pd
	timeString=arrData.DT.iloc[0]
	rowName=pd.to_datetime('2016-'+timeString[0]+'-'+timeString[1:3]+' '+timeString[3:5]+':'+timeString[5:7]+':'+timeString[7:])
# names the row as a timestamp with the month        day                  hour                  minute             second
	lineStat=pd.DataFrame('-',index=[rowName],columns=line)
	for station in line: #repeat the below process for every station on the line
		trains2consider=arrData.loc[lambda df: df.Loc==station].loc[lambda df: df.Des.isin(destList)] #pull out the trains at that station heading toward the destinations
		if len(trains2consider.index)>0: #If you found a train
			lineStat.loc[rowName][station]=trains2consider.Lin.iloc[0]+':'+trains2consider.Min.iloc[0] #set the station status to the color and ETA of the first arriving train
	return lineStat

def allLNtoNE4(arrData): #all of the lines to the North and East during Surge 4
    import pandas as pd
    #these are the lines to examine. lowercase letters (w,e,n,s,c) are the ordinal directions and central. Uppercase letters are the lines
    lineList=[wOEnd, wSEnd, SOLine, ['C07'], BArlCem, SOBLine, eOLine, SBLine, sGLine, cGYLine, nGYEnd, wRedEnd, cRedLine, eRedEnd]
    #these are the destinations along those lines. D13=New Car... G05=Largo E01=Mt Vernon E10=Greenbelt B08=Silver Spring B11=Glenmont    
    destList=[['D13'],['G05'],['D13','G05'],['G05','E01','E10'],['G05'],['G05','D13','G05'],['D13'],['G05'],['E10'],['E01','E10'],['E10'],['B11'],['B08','B11'],['B11']]
    LNlist=[]
    for num in range(len(lineList)):
        LNlist.append(lineNextDF(lineList[num], destList[num], arrData)) #run for each line and destination
    return pd.concat(LNlist, axis=1, join='outer') #then join them all together

def allLNtoSW4(arrData): #all of the lines to the South and West during Surge 4
    import pandas as pd
    #these are the lines to examine. lowercase letters (w,e,n,s,c) are the ordinal directions and central. Uppercase letters are the lines
    lineList=[wOEnd, wSEnd, SOLine, ['C07'], BArlCem, SOBLine, eOLine, SBLine, sGLine, cGYLine, nGYEnd, wRedEnd, cRedLine, eRedEnd]
    lineList.reverse() #reverse the list
    #these are the destinations along those lines. K08=Vienna N06=Wiehle C08=Pentagon City F11=Branch Ave A15=Shady Gr A11=Grovesnor
    destList=[['K08'],['N06'],['K08','N06'],['C08'],['C08'],['K08','C08','N06'],['K08'],['C08','N06'],['F11'],['F11', 'C08'],['F11', 'C08'],['A15'],['A11','A15'],['A15']]
    lineList.reverse() #reverse the list
    LNlist=[]
    for num in range(len(lineList)):
        lineList[num].reverse() #reverse the order of the stations in the list
        LNlist.append(lineNextDF(lineList[num], destList[num], arrData)) #run for each line and destination
    return pd.concat(LNlist, axis=1, join='outer') #then join them all together

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
	timesPD=arrData.DT.value_counts().index #pull out each time and call it timesPD
	lineStats=lineNextDF(line, destList, arrData.loc[lambda df: df.DT==timesPD[0]]) #save the first status
	for num in range(1,len(timesPD)): #for each time
  		lineStats=lineStats.append(lineNextDF(line, destList, arrData.loc[lambda df: df.DT==timesPD[num]])) #add the data for that time
	engine.connect().close() 
	print(time.strftime("%a, %d %b %Y %H:%M:%S"))
	return lineStats

def trainBuild(lineStat,startTime): #determines how long it took the train arriving after startTime to reach every station and returns it as one row data frame
	import pandas as pd
	timeRow=list(lineStat.index).index(startTime) #finds the row number from lineStat labeled startTime and calls it timeRow
	specTrain=pd.DataFrame('0',index=[startTime],columns=['Col']+list(lineStat.columns)) #creates a one row DataFrame with the color and arrival times
	while timeRow<len(lineStat.index)-1 and (len(lineStat.iloc[timeRow][0])<6 or lineStat.iloc[timeRow][0][-3:]!='BRD'): #while timeRow is in bounds and no train is boarding, 
          timeRow+=1 #go to the next line
	skipRows=timeRow-list(lineStat.index).index(startTime) #skipRows is the number of rows to skip the next time it looks for a train
	if timeRow>=len(lineStat.index): #if you get to the end,
             return [specTrain, skipRows] #just return what you have
	specTrain.loc[startTime]['Col']=lineStat.iloc[timeRow][0][:2] #fills in the color, which is stored as the first two letters in the status
	timeDif=lineStat.index[timeRow]-startTime #set timeDif to the diffence between arrival at this station and startTime
	specTrain.loc[startTime][lineStat.columns[0]]=str(timeDif.seconds) #store timeDif as seconds (converted into a string)
	for stationNum in range(1,len(lineStat.columns)): #this fills in the difference arrival time for every station
         isTrainBoarding=False
         while timeRow<(len(lineStat.index)-1) and not isTrainBoarding: #while timeRow is in bounds and the train is not boarding
            #The line below says that a train is boarding if either it has status "BRD" or it has status "ARR" and 20 seconds later the station is waiting for a different train 
            isTrainBoarding=lineStat.iloc[timeRow][stationNum]==(specTrain.loc[startTime]['Col']+":BRD") or (lineStat.iloc[timeRow][stationNum]==(specTrain.loc[startTime]['Col']+":ARR") and (lineStat.iloc[timeRow+1][stationNum][:2]!=specTrain.loc[startTime]['Col']))
            timeRow+=1 #go to the next line
         if timeRow>=len(lineStat.index)-1: #if you get to the end,
             return [specTrain, skipRows] #just return what you have
         timeDif=lineStat.index[timeRow]-startTime #set timeDif to the diffence between arrival at this station and startTime
         specTrain.loc[startTime][lineStat.columns[stationNum]]=str(timeDif.seconds) #store timeDif as seconds (converted into a string)
         if stationNum<len(lineStat.columns)-1: #if you found a trains, go down a certain number of rows before checking the next station
             if lineStat.columns[stationNum] in minDist.keys() and lineStat.columns[stationNum+1] in minDist.keys(): #if both stations are in minDist
                 timeRow+=minDist[lineStat.columns[stationNum]][lineStat.columns[stationNum+1]]['weight'] #go down the number of rows recorded in minDist
             else:
                 timeRow+=2 #if the connection isn't in minDist, go down two rows
	return [specTrain, skipRows]

def trainTable(lineStat): #returns a table listing the trains by start time, color and the time they took to reach a given station
    import pandas as pd
    [masterTable,rowNum]=trainBuild(lineStat,lineStat.index[0]) #builds the first row and lets it now how many rows to go forward to get to the next train arrival
    currentColor=masterTable.iloc[0][0] #record the color of the first train as currentColor
    newTrain=pd.DataFrame('1',index=['0'],columns=['Col']+list(lineStat.columns))
    while rowNum<len(lineStat.index) and newTrain.iloc[0][-1]!='0': #keep going as long as there's data to analyze and each train gets to the end
        while rowNum<len(lineStat.index)-1 and lineStat.iloc[rowNum][0]==currentColor+':BRD': #while the train (with currentColor) is boarding,
            rowNum+=1 #go to the next row
        [newTrain, skipRows]=trainBuild(lineStat,lineStat.index[rowNum]) #once you've gotten to a new train arrival, record it as newTrain and note the rows to skip 
        if newTrain.iloc[0][-1]=='0' or (newTrain.index[0]-masterTable.index[-1]).seconds>int(masterTable.iloc[-1][-1])-int(newTrain.iloc[0][-1]): #if you've reached the end or newTrain arrived at the last station after the last train
            masterTable=masterTable.append(newTrain) #append newTrain to the masterTable
        else: #but if that's not the case, something went wrong with the last train
            masterTable=pd.concat([masterTable.iloc[:][:-1],newTrain]) #replace the last row of masterTable with the data for newTrain
        currentColor=masterTable.iloc[-1][0] #xchange currentColor to the color of the train that just boarded
        rowNum+=skipRows+1 #skip ahead to the next train
    return masterTable

#these are the lines to examine. lowercase letters (w,e,n,s,c) are the ordinal directions and central. Uppercase letters (O,S,B,Y,G) and Red are the lines
wOEnd=['K07','K06','K05']
wSEnd=['N04','N03','N02','N01']
SOLine=['K04','K03','K02','K01']
wBEnd=['J02']
sYEnd=['C14','C15']
BYLine=['C07','C08','C09','C10','C12','C13']
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
 'B07': {'B06': {'weight': 5}, 'B08': {'weight': 4}},
 'B08': {'B07': {'weight': 4}, 'B09': {'weight': 6}},
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
 'D08': {'D07': {'weight': 4}, 'G01': {'weight': 8}},
 'D09': {'D10': {'weight': 4}},
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