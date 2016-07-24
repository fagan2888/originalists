# -*- coding: utf-8 -*-
"""
Created on Thu Jul 21 17:32:25 2016

@author: pbw50
"""

import pandas as pd
from sklearn import linear_model
from sklearn.cross_validation import train_test_split


df = pd.read_csv('C:/Users\pbw50\Desktop\ORsurge34trainsNE (1).csv')

#prepare a dataframe with column headings.  Will be used to translate raw data into features. 
cols=pd.DataFrame(df.columns.values)

# The first four functions create features.  The features are the locations, line color, and train starting time.  
#Time feature
def f1(data):
    lis4=[]
    for i in range(1):
        lis4.append(df.iloc[:,i])
    return lis4
#Line Color Feature
def f2(data):
    lis5=[]
    for i in range(2):
        lis5.append(df.iloc[:,i])
    return lis5
#First Location 
def f3(data):
    lis2=[]
    for i in range(2,25):  
        lis2.append(cols.loc[i,0])      
    return  lis2
#Second Location
def f4(data):
    lis3=[]
    for i in range(2,26):      
        lis3.append(cols.loc[i,0])    
    return  lis3
    
#Labels - the actual time it took to for a train to get from one stop to the next in order. 
def labels(data):
    lis1=[]
    for i in range(2,25):  
        lis1.append(df.iloc[:,i]-df.iloc[:,i+1])      
    return  lis1
#Final Dataset  Translates seconds into minutes. 
def final(data):
    feature1 = f1(df)
    feature2 = f2(df)
    feature3 = f3(df) 
    feature4 = f4(df)   
    lbl = labels(df)
    df0 = pd.DataFrame([])
    for i in range(0,23):        
        d = {'feature1': feature1[0],'feature2': feature2[1],'feature3': feature3[i], 
        'feature4': feature4[i+1],'label': abs(lbl[i])/60}    
        df1=pd.DataFrame(d)
        df0=df0.append(df1)      
    return df0

#create label row for model fitting.  
y = final(df).ix[:,-1]
#create features for model fitting. Translate categorical varibles to numbers. 
X = pd.get_dummies(final(df).ix[:,0:3])

#create 90/10 random split. 
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.10, random_state=1)

#Print all shapes for sets.  Column numbers much match. 
print(X_test.shape)
print(y_test.shape)
print(X_train.shape)
print(y_train.shape)

#Create and fit model.
clf = linear_model.LinearRegression()
clf.fit(X_train,y_train)
#feed model test set. 
prediction = pd.DataFrame(clf.predict(X_test)) 
#create dataframe to juxtapose actual vs predicted. 
final_df = pd.DataFrame([])
final_df["Actual"]=y_test
final_df["Predicted"]=prediction


X_test.to_excel('C:/Users/pbw50/Desktop/predict.xlsx')




