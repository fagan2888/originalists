# -*- coding: utf-8 -*-
"""
Created on Tue Aug 16 21:01:58 2016

@author: pbw50
"""

import pandas as pd
from sklearn import linear_model
from sklearn.cross_validation import train_test_split
from sklearn import preprocessing
from sklearn.linear_model import ElasticNet, Lasso
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import seaborn as sns
from sklearn.cross_validation import KFold
import os
import json
import time
import pickle
import requests
from sklearn import metrics

x=datetime.today()

y=x.replace(day=x.day+1, hour=1, minute=0, second=0, microsecond=0)


OrangelineSW = pd.read_csv('C:\Users\pbw50\Documents\data\metro\ORsurge2trainsSW.csv')
RedlineNE = pd.read_csv('C:\Users\pbw50\Documents\data\metro\RDsurge34trainsNE.csv')
#dfBL = pd.read_csv('C:/Users\pbw50\Desktop\BLsurge34trainsNE.csv')
#dfGR = pd.read_csv('C:/Users\pbw50\Desktop\GRsurge34trainsNE (1).csv')
#dfSV = pd.read_csv('C:/Users\pbw50\Desktop\SVsurge34trainsNE (1).csv')
#dfYL = pd.read_csv('C:/Users\pbw50\Desktop\YLsurge34trainsNE.csv')

print(OrangelineSW.columns)

RedlineNE.columns = ["Time_Start",'Line','Rockville', 'Twinbrook','White Flint',"Grosvenor-Strathmore",
                "Medical Center","Bethesda",
                "Friendship Heights","Tenleytown-AU","Van Ness-UDC","Cleveland Park","Woodley Park-Zoo/Adams Morgan",
                "Dupont Circle","Farragut North","Metro Center","Gallery Pl-Chinatown","Judiciary Square","Union Station",
                "Rhode Island Ave-Brentwood","Brookland-CUA","Fort Totten","Takoma","Silver Spring","Forest Glen","Wheaton",
                "Glenmont"]

OrangelineSW.columns = ["Time_Start",'Line',
                 "Capitol South","Federal Center SW","L'Enfant Plaza", "Smithsonian","Federal Triangle","Metro Center",
                 "McPherson Square","Farragut West","Foggy Bottom-GWU","Rosslyn","Court House",
                 "Clarendon","Virginia Square-GMU","Ballston-MU","East Falls Church","West Falls Church-VT/UVA","Dunn Loring-Merrifield"]                    


enterTrain = input("Type Train Exactly as it Appears and Press Enter:   \n Choose from the following:\n RedlineNE:\n OrangeLine:\n Enter Train Here:  ")
df = enterTrain
line = df.Line.unique()[0]

print(line)

df.Time_Start = df.Time_Start.astype('datetime64[h]')

#prepare a dataframe with column headings.  Will be used to translate raw data into features. 
cols=pd.DataFrame(df.columns.values)

#set line parameters.  This is needed to cosomize the features for each color line and csv. 


# The first four functions create features.  The features are the locations, line color, and train starting time.  
def f1(data):
    lis1=[]
    for i in range(1):
        lis1.append(df.iloc[:,i])
    return lis1
#Line Color Feature
def f2(data):
    lis2=[]
    for i in range(2):
        lis2.append(df.iloc[:,i])
    return lis2
#First Location 
def f3(data):
    lis3=[]
    x=0
    if line=="RD":
        x=25
    if line=="OR":
        x=18
    for i in range(2,x):  
        lis3.append(cols.loc[i,0])      
    return  lis3
#Second Location
def f4(data):
    lis4=[]
    x=0
    if line=="RD":
        x=26
    if line=="OR":
        x=18
        print(x)
    for i in range(2,x):      
        lis4.append(cols.loc[i,0])    
    return  lis4   
    
#print(f4(df))

def f5(data):
    lis6=[]
    x=0
    if line=="RD":
        x=25
    if line=="OR":
        x=17
        print(x)
    for i in range(2,x):
        lis6.append(abs(df.iloc[:,i]-df.iloc[:,i+2])/60)
    return lis6

#print(f5(df))
#Labels - the actual time it took to for a train to get from one stop to the next in order. 
def label1(data):
    lis5=[]
    x=0
    if line=="RD":
        x=26
    if line=="OR":
        x=18
    for i in range(2,x):  
        lis5.append(df.iloc[:,i]-df.iloc[:,i+1])      
    return  lis5
      
#Final Dataset  Translates seconds into minutes. #Removes all instances where the label falls 3 stadard 
#deviations away from the norm. 
        
def final(data):
    feature1 = f1(df)
    feature2 = f2(df)
    feature3 = f3(df) 
    feature4 = f4(df) 
    feature5 = f5(df)
#    print(feature5)
    lbl = label1(df)
  
    df0 = pd.DataFrame([])
    x=0
    if line=="RD":
        x=23
        print(x)
    if line =="OR":
        x=15
    for i in range(0,x):        
        d = {'feature1': feature1[0],'feature2': feature2[1],'feature3': feature3[i], 
        'feature4': feature4[i+1], 'feature5': feature5[i],'label': abs(lbl[i])/60}    
        df1=pd.DataFrame(d)        
        df0=df0.append(df1)
        df0.reset_index() 
    return df0[(df0.label>0)&(np.abs(df0.label-df0.label.mean())<=3*df0.label.std())].round(2)

#create label row for model fitting.
final_data=pd.DataFrame(final(df).reset_index())  
final_data.feature1 = final_data.feature1
#print(final_data.feature1)


def Kfold(data):
#    scores = {'precision':[], 'recall':[], 'accuracy':[], 'f1':[]}
    for train, test in KFold(final_data.shape[0], n_folds=12, shuffle=True,random_state=1):
        X_train, X_test = pd.get_dummies(final_data.ix[:,2:5]), pd.get_dummies(final_data.ix[:,2:5])
        y_train, y_test = final_data.ix[:,-1], final_data.ix[:,-1]
        lin_reg = linear_model.LinearRegression(normalize='l1')
        lin_reg.fit(X_train,y_train)
        expected  = y_test
        predicted = lin_reg.predict(X_test)

    print(lin_reg.score(X_test, y_test).mean())
    print(predicted.mean())
#    print(y_train)
#    print(predicted[30])
        
print(Kfold(final_data))

g = sns.factorplot(x="label", 
                   y="feature4",
                   hue="feature2",                   
                   data=final_data,             
                   size=8, kind="bar", palette="muted")

g.set_ylabels("Line")
