# -*- coding: utf-8 -*-
"""
Created on Thu Jul 21 17:32:25 2016

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

Station_to_Station = pd.read_csv('C:\Users\pbw50\Desktop\station_to_station.csv', usecols=['CompositeMiles','Start Location','End Location','RailTime'])

OrangelineSW = pd.read_csv('C:\Users\pbw50\Documents\data\metro\ORsurge2trainsSW.csv')
RedlineNE = pd.read_csv('C:\Users\pbw50\Documents\data\metro\RDsurge34trainsNE.csv')
#dfBL = pd.read_csv('C:/Users\pbw50\Desktop\BLsurge34trainsNE.csv')
#dfGR = pd.read_csv('C:/Users\pbw50\Desktop\GRsurge34trainsNE (1).csv')
#dfSV = pd.read_csv('C:/Users\pbw50\Desktop\SVsurge34trainsNE (1).csv')
#dfYL = pd.read_csv('C:/Users\pbw50\Desktop\YLsurge34trainsNE.csv')

#print(RedlineNE.head())
#print(Station_to_Station.head())


RedlineNE.columns = ["Time_Start",'Line','Rockville', 'Twinbrook','White Flint',"Grosvenor-Strathmore",
                "Medical Center","Bethesda",
                "Friendship Heights","Tenleytown-AU","Van Ness-UDC","Cleveland Park","Woodley Park-Zoo/Adams Morgan",
                "Dupont Circle","Farragut North","Metro Center","Gallery Pl-Chinatown","Judiciary Square","Union Station","NOMA",
                "Rhode Island Ave-Brentwood","Brookland-CUA","Fort Totten","Takoma","Silver Spring","Forest Glen","Wheaton",
                ]
             
#dfOR.columns = ["Time_Start",'Line',"Vienna/Fairfax-GMU","Dunn Loring-Merrifield","West Falls Church-VT/UVA","East Falls Church",
#                "Ballston-MU","Virginia Square-GMU","Clarendon","Court House","Rosslyn","Farragut West",
#                "McPherson Square","Metro Center","Federal Triangle","Smithsonian","L'Enfant Plaza","Federal Center SW",
#                "Capitol South","Eastern Market","Potomac Ave","Stadium-Armory","Minnesota Ave",
#                "Deanwood","Cheverly","Landover"]                
#                
OrangelineSW.columns = ["Time_Start",'Line',
                 "Capitol South","Federal Center SW","L'Enfant Plaza", "Smithsonian","Federal Triangle","Metro Center",
                 "McPherson Square","Farragut West","Foggy Bottom-GWU","Rosslyn","Court House",
                 "Clarendon","Virginia Square-GMU","Ballston-MU","East Falls Church","West Falls Church-VT/UVA","Dunn Loring-Merrifield"]                    
               
#print(OrangelineSW.head())

#enterTrain = input("Type Train Exactly as it Appears and Press Enter:   \n Choose from the following:\n RedlineNE:\n OrangeLine:\n Enter Train Here:Re   ")
df = RedlineNE
line = df.Line.unique()[0]
print(line)

df.Time_Start = df.Time_Start.astype('datetime64')



df['TimeStamp'] = pd.Series([val.time() for val in df.Time_Start]).astype(str)

#prepare a dataframe with column headings.  Will be used to translate raw data into features. 
cols=pd.DataFrame(df.columns.values)
count=cols.count()
count1=count-1

#set line parameters.  This is needed to cosomize the features for each color line and csv. 

# The first four functions create features.  The features are the locations, line color, and train starting time.  
def f1(data):
    lis1=[]
    for i in range(count1,count):
        lis1.append(df.iloc[:,i])
    return lis1
#print(f1(df))
#Line Color Feature
def f2(data):
    lis2=[]
    for i in range(2):
        lis2.append(df.iloc[:,i])
    return lis2
#print(f2(df))
#First Location 
def f3(data):
    lis3=[]
    x=0
    if line=="RD":
        x=26
    if line=="OR":
        x=18
    for i in range(2,x):  
        lis3.append(cols.loc[i,0])      
    return  lis3

#print(f3(df))
#Second Location
def f4(data):
    lis4=[]
    x=0
    if line=="RD":
        x=27
    if line=="OR":
        x=18
        print(x)
    for i in range(2,x):      
        lis4.append(cols.loc[i,0])    
    return  lis4   
    
#print(f4(df))


#print(Station_to_Station.columns)

    

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
    
#    feature5 = f5(df)
    lbl = label1(df)
  
    df0 = pd.DataFrame([])
    x=0
    if line=="RD":
        x=24

    if line =="OR":
        x=15
    for i in range(0,x):        
        d = {'Time': feature1[0],'Line': feature2[1],'Start Location': feature3[i], 
        'End Location': feature4[i+1],'label': abs(lbl[i])/60}    
        df1=pd.DataFrame(d)        
        df0=df0.append(df1)
        df0.reset_index() 
    return df0[(df0.Line!='-')&(df0.label>0)&(np.abs(df0.label-df0.label.mean())<=3*df0.label.std())]

#print(final(df).head())
#create label row for model fitting.
final_data1=pd.DataFrame(final(df).reset_index())  
final_data = pd.merge(Station_to_Station[[0,2,3]],final_data1.ix[:,1:6], on=['Start Location', 'End Location'])
#print(final_data.ix[:,0:5])
#final_data.feature1 = final_data.feature1
#print(final_data.feature1)


#Data below will be used to join to the predicted results. 
forjoin = pd.merge(Station_to_Station[[0,2,3,1]],final_data1.ix[:,1:5], on=['Start Location', 'End Location'])
print(forjoin.head())
#test = pd.merge(Station_to_Station[[1,4,0,5,3]], final_data.ix[:,1:6], on=['feature3', 'feature4'])
#print(test.describe())
#y=final_data.ix[:,-1]
#test.to_excel('C:/Users/pbw50/Desktop/test.xlsx')
#create features for model fitting. Translate categorical varibles to numbers.
#X = pd.get_dummies(final_data.ix[:,1:5])

#X_scaled = preprocessing.scale(X)

def Kfold(data):
    start  = time.time() # Start the clock! 
    scores = {'R2 Linear Regressor':[], 'R2 SGD Regressor':[], 'R2 Elastic Net':[], 'R2 Lasso':[]}
    for train, test in KFold(final_data.shape[0], n_folds=12, shuffle=True,random_state=1):
        X_train, X_test = pd.get_dummies(final_data.ix[:,0:5]), pd.get_dummies(final_data.ix[:,0:5])
        y_train, y_test = final_data.ix[:,-1], final_data.ix[:,-1]
        
        #Linear Regression
        lin_reg = linear_model.LinearRegression(normalize='l1')
        lin_reg.fit(X_train,y_train)
                
        #Stochastic Gradient 
        sgd_reg = linear_model.SGDRegressor(penalty='l1')
        sgd_reg.fit(X_train,y_train)
        
        #Elastic Net
        enet_reg = linear_model.ElasticNetCV()
        enet_reg.fit(X_train,y_train)
        
        lass_reg = linear_model.LassoCV()
        lass_reg.fit(X_train,y_train)
             
        expected  = y_test
        predicted_linear_Regressor = lin_reg.predict(X_test)
        predicted_SGD_Regressor = sgd_reg.predict(X_test)
        enet_prediction  = enet_reg.predict(X_test)
        lass_prediction = lass_reg.predict(X_test)        

        scores['R2 Linear Regressor'].append(metrics.r2_score(expected, predicted_linear_Regressor,multioutput='variance_weighted' ))
        scores['R2 SGD Regressor'].append(metrics.r2_score(expected, predicted_SGD_Regressor,multioutput='variance_weighted' ))
        scores['R2 Elastic Net'].append(metrics.r2_score(expected, enet_prediction ))
        scores['R2 Lasso'].append(metrics.r2_score(expected, lass_prediction ,multioutput='variance_weighted' ))

    print "Build and Validation of {} \ntook {:0.3f} seconds".format(final_data.ix[:,1:5], time.time()-start)
    print('\n')
    print "Validation scores are as follows:\n"
    print pd.DataFrame(scores).mean()

    final_df = pd.DataFrame([])
    
    final_df["Linear Regr Predicted"]=predicted_linear_Regressor
    final_df["SGD Regr Predicted"]=predicted_SGD_Regressor
    final_df["Elastic Net Predicted"]=enet_prediction
    final_df['lasso Predicted']=lass_prediction
    
    final_df["Actual"]=y_test
    result = pd.concat([forjoin, final_df], axis=1, join='inner')
    result.to_excel('C:/Users/pbw50/Desktop/RD.xlsx')
print(Kfold(final_data))

#create 90/10 random split. 


#Print all shapes for sets. Column numbers much match. 
#print(X_test.shape)
#print(y_test.shape)
#print(X_train.shape)
#print(y_train.shape)
#


#Create models.
#lin_reg = linear_model.LinearRegression(normalize='l1')
#sgd_reg = linear_model.SGDRegressor(penalty='l1')

#
##Fit models.
#lin_reg.fit(X_train,y_train)
#sgd_reg.fit(X_train,y_train)
#enet_reg.fit(X_train,y_train)
#lass_reg.fit(X_train,y_train)
#
##predictions. 
#lin_prediction = lin_reg.predict(X_test)
#sgd_prediction = sgd_reg.predict(X_test)
#enet_prediction = enet_reg.predict(X_test)
#lass_prediction = lass_reg.predict(X_test)
##diplay Results: 

# Explained variance score: 1 is perfect prediction
#print("Linear_Regression Results:")
#print("Residual sum of squares: %.2f"
#      % np.mean((lin_reg.predict(X_test) - y_test) ** 2))
#print('Variance score: %.2f' % lin_reg.score(X_test, y_test))
#print("")
#
#Re
#print("Stochastic Gradient Descent Results")
##print(sgd_reg.score(X_test,y_test))
#print("Residual sum of squares: %.2f"
#      % np.mean((sgd_reg.predict(X_test) - y_test) ** 2))
#print('Variance score: %.2f' % sgd_reg.score(X_test, y_test))
#print("")
#
#print('')
#
#print("Elastic Net Results:")
#print(enet_reg.score(X_test,y_test))
#print("Residual sum of squares: %.2f"
#      % np.mean((enet_reg.predict(X_test) - y_test) ** 2))
#print('Variance score: %.2f' % enet_reg.score(X_test, y_test))
#
#print('')
#print("Lasso Results:")
#print(lass_reg.score(X_test,y_test))
#print("Residual sum of squares: %.2f"
#      % np.mean((lass_reg.predict(X_test) - y_test) ** 2))
#print('Variance score: %.2f' % lass_reg.score(X_test, y_test))



#create dataframe to juxtapose actual vs predicted. 
#final_df = pd.DataFrame([])
#final_df["Actual"]=y_test
#final_df["Linear Regr Predicted"]=lin_prediction
#final_df["SGD Regr Predicted"]=sgd_prediction
#final_df["Elastic Net Regr Predicted"]=enet_prediction
#final_df["Lasso Predicted"]=lass_prediction
##join predicted with original dataset for display
#
#
#result = pd.concat([forjoin, final_df], axis=1, join='inner')
#
#result.to_excel('C:/Users/pbw50/Desktop/RD.xlsx')



#g = sns.factorplot(x="label", 
#                   y="feature4",
#                   hue="feature2",                   
#                   data=final_data,             
#                   size=8, kind="bar", palette="muted", color="orange")
#
#g.set_ylabels("Line")
#print(g)


#
#def fit_and_evaluate(final_data, model, label, **kwargs):
#    """
#    Because of the Scikit-Learn API, we can create a function to
#    do all of the fit and evaluate work on our behalf!
#    """
#    start  = time.time() # Start the clock! 
#    scores = {'precision':[], 'recall':[], 'accuracy':[], 'f1':[]}
#    
#    for train, test in KFold(final_data.shape[0], n_folds=12, shuffle=True):
#        X_train, X_test = pd.get_dummies(final_data.ix[:,2:5]), pd.get_dummies(final_data.ix[:,2:5])
#        y_train, y_test = final_data.ix[:,-1], final_data.ix[:,-1]
#        
#        estimator = model
#        estimator.fit(X_train, y_train)
#        
#        expected  = y_test
#        predicted = estimator.predict(X_test)
#        
#        # Append our scores to the tracker
#        scores['precision'].append(metrics.precision_score(expected, predicted, average="weighted"))
#        scores['recall'].append(metrics.recall_score(expected, predicted, average="weighted"))
#        scores['accuracy'].append(metrics.accuracy_score(expected, predicted))
#        scores['f1'].append(metrics.f1_score(expected, predicted, average="weighted"))
#
#    # Report
#    print "Build and Validation of {} took {:0.3f} seconds".format(label, time.time()-start)
#    print "Validation scores are as follows:\n"
#    print pd.DataFrame(scores).mean()
#    
#    # Write official estimator to disk
##    estimator = model
##    estimator.fit(X_train, y_train)
##    
##    outpath = label.lower().replace(" ", "-") + ".pickle"
##    with open(outpath, 'w') as f:
##        pickle.dump(estimator, f)
##
##    print "\nFitted model written to:\n{}".format(os.path.abspath(outpath))
#
#
#
#
#
#print(fit_and_evaluate(final_data,linear_model.LinearRegression(), "Linear" ))
#





#
#clf = linear_model.Ridge(fit_intercept=False)
#errors = []
#
#
#alphas = np.logspace(-10, -2, n_alphas)
#
#
#
#
#
#for alpha in alphas:
#    splits = tts(X_train,y_train, test_size=0.1)
#    X_train, X_test, y_train, y_test = splits
#    clf.set_params(alpha=alpha)
#    clf.fit(X_train, y_train)
#    error = mean_squared_error(y_test, clf.predict(X_test))
#    errors.append(error)
#
#axe = plt.gca()
#axe.plot(alphas, errors)
#plt.show()





#dataset = Bunch(
# data=data,
# target=target,
# filenames=filenames,
# target_names=target_names,
# feature_names=feature_names,
# DESCR=DESCR
#)
#
#






#fig, ax = plt.subplots()
#ax.scatter(y_test, prediction)
#ax.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'k--', lw=4)
#ax.set_xlabel('Measured')
#ax.set_ylabel('Predicted')
#plt.show()

#plt.scatter(prediction, y_test,  color='black')
#plt.plot(X_test, lin_reg.predict(X_test), color='blue',
#         linewidth=3)
#
#plt.xticks(())
#plt.yticks(())
#
#plt.show()











