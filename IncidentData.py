# -*- coding: utf-8 -*-
"""
Created on Sat Jun 25 09:39:03 2016

@author: pbw50
"""
import pandas as pd
import httplib, urllib, json
import time
from datetime import datetime
from sqlalchemy import create_engine
from datetime import datetime
from threading import Timer

x=datetime.today()

y=x.replace(day=x.day+1, hour=1, minute=0, second=0, microsecond=0)
z=x.replace(day=x.day+1, hour=17, minute=0, second=0, microsecond=0)

print(y)
print(z)
delta_t=y-x
delta_s=z-x
print(delta_t)

secs=delta_t.seconds+1
secs1=delta_s.seconds+1

        
headers = {'api_key': 'b09803895cbf43a99c32945484c858a8'}        
params = urllib.urlencode({})        
try:
   conn = httplib.HTTPSConnection('api.wmata.com')
   conn.request("GET", "/Incidents.svc/json/Incidents?%s" % params, "{body}", headers)
   response = conn.getresponse()
   data = response.read()
   
except Exception as e:
   print("[Errno {0}] {1}".format(e.errno, e.strerror))
    # Create the database connection using the "create_engine" method from the sqlalchemy class.
    # In order for this to work, you nedd to pass in the DB User name "Original" the Password "tolistbtGU!" the path cc95... and the database name "PWTEST".
engine = create_engine('postgresql+psycopg2://Original:tolistbtGU!@teamoriginal.ccc95gjlnnnc.us-east-1.rds.amazonaws.com:5432/PWTEST')    
def runEvening():
    count = 0
    print(x.minute)
    while (count<22):
        z=datetime.today()                
        Data = json.loads(data)
        df = pd.DataFrame(Data["Incidents"])
        df["Time"] = datetime.today()        
        print(df.count())        
        df.to_sql('Incidents', engine, if_exists= 'append')
        time.sleep(23)
        count = z.hour
        print(count)
        print("test")
        if(count==22):
            break
 
def runMorning():
    count = 0
    print(x.minute)
    while ( count < 10):
        z=datetime.today()     
        print(datetime.minute)                     
        Data = json.loads(data)    
        df = pd.DataFrame(Data["Incidents"])
        df["Time"] = datetime.today()        
        print(df.count())        
        df.to_sql('Incidents', engine, if_exists= 'append')
        time.sleep(23)
        count = z.hour
        print(count)
        if(count==10):
            break
                           
def  main():    
    t = Timer(secs, runMorning)
    s = Timer(secs1, runEvening)
    t.start()
    s.start()

main()

            

  
    

