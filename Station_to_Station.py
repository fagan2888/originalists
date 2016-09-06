# -*- coding: utf-8 -*-
"""
Created on Fri Sep 02 18:57:09 2016

@author: pbw50
"""

import httplib, urllib, base64
import pandas as pd
import json
import numpy as np
headers = {
    # Request headers
    'api_key': 'b09803895cbf43a99c32945484c858a8',
}



params = urllib.urlencode({
    # Request parameters
#    'FromStationCode': 'A14',
#    'ToStationCode': 'A13',
      
})

try:
    conn = httplib.HTTPSConnection('api.wmata.com')
    conn.request("GET", "/Rail.svc/json/jSrcStationToDstStationInfo?%s" % params, "{body}", headers)
    response = conn.getresponse()
    data = response.read()
   
    
    conn.close()
except Exception as e:
    print("[Errno {0}] {1}".format(e.errno, e.strerror))

#print(data)
Data = json.loads(data)    

#df = pd.read_json(data)
#print(df.head())

df = pd.DataFrame.from_dict(Data["StationToStationInfos"])
final_df = df[[0,4,1,3]]
A_stations = final_df[(    ((~final_df.SourceStation.str.contains('B11')) )&(final_df.DestinationStation!='B11'))\
&((final_df.SourceStation.str.contains('B')&final_df.DestinationStation.str.contains('B')))|\
(((~final_df.SourceStation.str.contains('A15')) )&(final_df.DestinationStation!='A15'))&\
((final_df.SourceStation.str.contains('A'))&(final_df.DestinationStation.str.contains('A')))]

def source_trans(data):
    if(data.SourceStation == "A14"):
        return "Rockville"
    if(data.SourceStation == "A13"):
        return "Twinbrook"   
    if(data.SourceStation == "A12"):
        return "White Flint"
    if(data.SourceStation == "A11"):
        return "Grosvenor-Strathmore"   
    if(data.SourceStation == "A10"):
        return "Medical Center"
    if(data.SourceStation == "A09"):
        return "Bethesda"                        
    if(data.SourceStation == "A08"):
        return "Friendship Heights"                  
    if(data.SourceStation == "A07"):
        return "Tenleytown-AU"                  
    if(data.SourceStation == "A06"):
        return "Van Ness-UDC"                
    if(data.SourceStation == "A05"):
        return "Cleveland Park"                   
    if(data.SourceStation == "A04"):
        return "Woodley Park-Zoo/Adams Morgan"                
    if(data.SourceStation == "A03"):
        return "Dupont Circle"                 
    if(data.SourceStation == "A02"):
        return "Farragut North"                  
    if(data.SourceStation == "A01"):
        return "Metro Center"        
    if(data.SourceStation == "B01"):
        return "Gallery Pl-Chinatown"                  
    if(data.SourceStation == "B02"):
        return "Judiciary Square"       
    #####
    if(data.SourceStation == "B03"):
        return "Union Station"                  
    if(data.SourceStation == "B35"):
        return "NOMA"                  
    if(data.SourceStation == "B04"):
        return "Rhode Island Ave-Brentwood"                 
    if(data.SourceStation == "B05"):
        return "Brookland-CUA"                   
    if(data.SourceStation == "B06"):
        return "Fort Totten"               
    if(data.SourceStation == "B07"):
        return "Takoma"                 
    if(data.SourceStation == "B08"):
        return "Silver Spring"          
    if(data.SourceStation == "B09"):
        return "Forest Glen" 
    if(data.DestinationStation == "B10"):
        return "Wheaton"  
    else:
        return 0
     
def dest_trans(data):
    if(data.DestinationStation == "A14"):
        return "Rockville"
    if(data.DestinationStation == "A13"):
        return "Twinbrook"   
    if(data.DestinationStation== "A12"):
        return "White Flint"
    if(data.DestinationStation == "A11"):
        return "Grosvenor-Strathmore"   
    if(data.DestinationStation == "A10"):
        return "Medical Center"
    if(data.DestinationStation == "A09"):
        return "Bethesda"            
    if(data.DestinationStation == "A08"):
        return "Friendship Heights"                 
    if(data.DestinationStation == "A07"):
        return "Tenleytown-AU"
    if(data.DestinationStation == "A06"):
        return "Van Ness-UDC"         
    if(data.DestinationStation == "A05"):
        return "Cleveland Park"
    if(data.DestinationStation == "A04"):
        return "Woodley Park-Zoo/Adams Morgan"               
    if(data.DestinationStation == "A03"):
        return "Dupont Circle"        
    if(data.DestinationStation == "A02"):
        return "Farragut North"          
    if(data.DestinationStation == "A01"):
        return "Metro Center"                 
    if(data.DestinationStation == "B01"):
        return "Gallery Pl-Chinatown"                
    if(data.DestinationStation == "B02"):
        return "Judiciary Square"        
    #####
    if(data.DestinationStation == "B03"):
        return "Union Station"                  
    if(data.DestinationStation == "B35"):
        return "NOMA"                  
    if(data.DestinationStation == "B04"):
        return "Rhode Island Ave-Brentwood"                 
    if(data.DestinationStation == "B05"):
        return "Brookland-CUA"                   
    if(data.DestinationStation == "B06"):
        return "Fort Totten"                
    if(data.DestinationStation == "B07"):
        return "Takoma"                
    if(data.DestinationStation == "B08"):
        return "Silver Spring"                  
    if(data.DestinationStation == "B09"):
        return "Forest Glen"       
    if(data.DestinationStation == "B10"):
        return "Wheaton"  
    else:
        return 0   
        
A_stations["Source"]=A_stations.apply(source_trans, axis=1)

A_stations["Destination"]=A_stations.apply(dest_trans, axis=1)



print(A_stations.head())

print("\n"+"Source")
print(A_stations.SourceStation.unique())
print("\n"+"Destination")
print(A_stations.DestinationStation.unique())
A_stations.to_csv('C:/Users/pbw50/Desktop/station_to_station.csv')
#PeakTime = pd.concat([pd.DataFrame.from_dict(item, orient='index')for item in df.RailFare])
#
#
##print(PeakTime.loc['PeakTime'])
#
#df["PeakTime"]=PeakTime.loc['PeakTime']
#print(df[[1,4,0,5,3]])
