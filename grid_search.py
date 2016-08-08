#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import division
import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.metrics import precision_recall_curve

data = pd.read_table('force08.txt',sep='\t',header=0,na_values='NULL')
# data.to_csv('force07.csv')
data.loc[data.mhtl_roomquantity.isnull(),'mhtl_roomquantity'] = data.mhtl_maxroomnum_30d
data.loc[data.mhtl_roomquantity<=0,'mhtl_roomquantity'] = data.mhtl_maxroomnum_30d
data.loc[data.mhtl_roomquantity>=999,'mhtl_roomquantity'] = data.mhtl_maxroomnum_30d

data['mbroom_noroompct_efdt_30d'] = data['mbroom_submitordnum_noroom_efdt_30d'] / data['mbroom_submitordnum_efdt_30d']
data['mbroom_noroompct_30d'] = data['mbroom_submitordnum_noroom_30d'] / data['mbroom_submitordnum_30d']
data['mbroom_roomquantity_occupied'] = data['mbroom_nocancelroomnum_efdt_30d'] / data['mbroom_maxroomnum_30d']

data['mhtl_noroompct_efdt_30d'] = data['mhtl_submitordnum_noroom_efdt_30d'] / data['mhtl_submitordnum_efdt_30d']
data['mhtl_noroompct_30d'] = data['mhtl_submitordnum_noroom_30d'] / data['mhtl_submitordnum_30d']
data['mhtl_roomquantity_occupied'] = data['mhtl_nocancelroomnum_efdt_30d'] / data['mhtl_roomquantity']
data['mhtl_hold_ordnum_pct'] = data['mhtl_holdroom_ordnum_30d'] / data['mhtl_submitordnum_30d']

data['htl_noroompct_efdt_30d'] = data['htl_submitordnum_noroom_efdt_30d'] / data['htl_submitordnum_efdt_30d']
data['htl_noroompct_30d'] = data['htl_submitordnum_noroom_30d'] / data['htl_submitordnum_30d']
data['htl_roomquantity_occupied'] = data['htl_nocancelroomnum_efdt_30d'] / data['htl_maxroomnum_30d']

data['mbroom_force_noroompct_efdt_30d'] = data['mbroom_force_submitordnum_noroom_efdt_30d'] / data['mbroom_force_submitordnum_efdt_30d']
data['mbroom_force_noroompct_30d'] = data['mbroom_force_submitordnum_noroom_30d'] / data['mbroom_force_submitordnum_30d']
data['mbroom_force_roomquantity_pct'] = data['mbroom_force_nocancelroomnum_efdt_30d'] / data['mbroom_nocancelroomnum_efdt_30d']

data['mhtl_force_noroompct_efdt_30d'] = data['mhtl_force_submitordnum_noroom_efdt_30d'] / data['mhtl_force_submitordnum_efdt_30d']
data['mhtl_force_noroompct_30d'] = data['mhtl_force_submitordnum_noroom_30d'] / data['mhtl_force_submitordnum_30d']
data['mhtl_force_roomquantity_pct'] = data['mhtl_force_nocancelroomnum_efdt_30d'] / data['mhtl_nocancelroomnum_efdt_30d']

data['htl_force_noroompct_efdt_30d'] = data['htl_force_submitordnum_noroom_efdt_30d'] / data['htl_force_submitordnum_efdt_30d']
data['htl_force_noroompct_30d'] = data['htl_force_submitordnum_noroom_30d'] / data['htl_force_submitordnum_30d']
data['htl_force_roomquantity_pct'] = data['htl_force_nocancelroomnum_efdt_30d'] / data['htl_nocancelroomnum_efdt_30d']

data['ordd'] = pd.to_datetime(data['ordd'],format="%Y-%m-%d")
data['arrivaldays_before_holiday'] = np.zeros((data.shape[0],))
temp1 = data['ordd'] < pd.to_datetime('2016-04-02',format='%Y-%m-%d')
data.loc[temp1,'arrivaldays_before_holiday'] = ((pd.to_datetime('2016-04-02',format='%Y-%m-%d')-data.loc[temp1,'ordd'])/np.timedelta64(1,'D')).astype(int)
temp1 = (data['ordd'] > pd.to_datetime('2016-04-04',format='%Y-%m-%d')) & (data['ordd'] < pd.to_datetime('2016-04-30',format='%Y-%m-%d'))
data.loc[temp1,'arrivaldays_before_holiday'] = ((pd.to_datetime('2016-04-30',format='%Y-%m-%d')-data.loc[temp1,'ordd'])/np.timedelta64(1,'D')).astype(int)
temp1 = data['ordd'] > pd.to_datetime('2016-05-02',format='%Y-%m-%d')
data.loc[temp1,'arrivaldays_before_holiday'] = ((pd.to_datetime('2016-06-09',format='%Y-%m-%d')-data.loc[temp1,'ordd'])/np.timedelta64(1,'D')).astype(int)

data['hasroom'] = 1 - data['isnoroom']
data.loc[data.defrecommend<=0,'defrecommend'] = 0
data.loc[data.defrecommend>=10,'defrecommend'] = 0
# data['defrecommend'] /= 10.0

data['close_hour_per_time_30d'] = data['closehours_30d'] / data['closetimes_30d']
data['close_hour_per_time_efdt_30d'] = data['closehours_efdt_30d'] / data['closetimes_efdt_30d']

Xdat1 = data[["ordd","hasroom",
				"mbroom_noroompct_efdt_30d",
				"mbroom_noroompct_30d",
				"mbroom_roomquantity_occupied",
				"mhtl_noroompct_efdt_30d",
				"mhtl_noroompct_30d",
				"mhtl_roomquantity_occupied",
				"mhtl_hold_ordnum_pct",
				"htl_noroompct_efdt_30d",
				"htl_noroompct_30d",
				"htl_roomquantity_occupied",
				"mbroom_force_noroompct_efdt_30d",
				"mbroom_force_noroompct_30d",
				"mbroom_force_roomquantity_pct",
				"mhtl_force_noroompct_efdt_30d",
				"mhtl_force_noroompct_30d",
				"mhtl_force_roomquantity_pct",
				"htl_force_noroompct_efdt_30d",
				"htl_force_noroompct_30d",
				"htl_force_roomquantity_pct",
                "cityi","zonei","zonestari",
                "defrecommend",]].copy()

Xdat2 = data[[ 
              "mbroom_submitordnum_efdt_30d",
              "mbroom_submitordnum_30d",
              "mhtl_submitordnum_efdt_30d",
              "mhtl_submitordnum_30d",
              "htl_submitordnum_efdt_30d",
              "htl_submitordnum_30d",
              "mbroom_force_submitordnum_efdt_30d",
              "mbroom_force_submitordnum_30d",
              "mhtl_force_submitordnum_efdt_30d",
              "mhtl_force_submitordnum_30d",
              "htl_force_submitordnum_efdt_30d",
              "htl_force_submitordnum_30d",
              "ordadvanceday",
              "remainder_saletime",
              # "close_hour_per_time_30d",
              # "closetimes_30d",
              # "close_hour_per_time_efdt_30d",
              # "closetimes_efdt_30d",
              ]].copy()

def XScaler(X): # 均值归零，方差归一化
    mu = np.nanmean(X,axis=0) # nan不计算，其余的求平均，axis标示计算轴
    sigma = np.nanstd(X,axis=0)
    Xscl = (X-mu)/sigma
    return Xscl

Xdat2_scl = XScaler(Xdat2)
del Xdat2

from sklearn.preprocessing import LabelEncoder
le = LabelEncoder()
nonnumberic_columns = ["capp", "hotelbelongto"]
for feature in nonnumberic_columns:
  data[feature] = le.fit_transform(data[feature])

from sklearn.preprocessing import OneHotEncoder
one_hot_features = ["dayofweek","star","goldstar","ord_hour","ordadvanceday"] + nonnumberic_columns
one_hot_var = data[one_hot_features].copy()
enc = OneHotEncoder(sparse=False)
Xenc_2 = enc.fit_transform(one_hot_var)
Xenc_2 = pd.DataFrame(Xenc_2,index=data.index)

Xdat = pd.merge(Xdat1,Xdat2_scl,how='left',left_index=True,right_index=True)
Xdat = pd.merge(Xdat,Xenc_2,how='left',left_index=True,right_index=True)

#训练集测试集
idx_train = (Xdat.ordd>='2016-03-01') & (Xdat.ordd<='2016-04-30')
idx_test = (Xdat.ordd>='2016-05-01') & (Xdat.ordd<='2016-05-15')

Xtrain = Xdat.loc[idx_train,:]
ytrain = Xdat.loc[idx_train,'hasroom']
# ytrain = pd.DataFrame(ytrain)
Xtrain = Xtrain.drop('ordd',axis=1,inplace=False)
Xtrain = Xtrain.drop('hasroom',axis=1,inplace=False)

Xtest = Xdat.loc[idx_test,:]
ytest = Xdat.loc[idx_test,'hasroom']
# ytest = pd.DataFrame(ytest)
Xtest = Xtest.drop('ordd',axis=1,inplace=False)
Xtest = Xtest.drop('hasroom',axis=1,inplace=False)

a = open('grid_search.txt','r')
lines = len(a.readlines())
a.close()

skip = 0
for colsample_bytree in [i/10.0 for i in range(5,11)]:
  for subsample in [i/10.0 for i in range(5,11)]:
    for learning_rate in [i/100.0 for i in range(1,21)]:
      if skip < lines:
        skip += 1
        continue
      params = {'max_depth':3, 'learning_rate':learning_rate, 'n_estimators':1000,
        'objective':'binary:logistic', 'subsample': subsample, 'colsample_bytree':colsample_bytree,
        'missing':np.nan}
      clfXGB = xgb.XGBClassifier(**params)
      clfXGB.fit(Xtrain.fillna(-99).as_matrix(),ytrain.as_matrix(),early_stopping_rounds=30,
              eval_set=[(np.array(Xtest.fillna(-99)),ytest)],
              eval_metric = 'auc', verbose = True)
      pred_prob = clfXGB.predict_proba(Xtest)

      precision,recall,threshold = precision_recall_curve(ytest,pred_prob[:,1])
      rec = []
      for i,pre in enumerate(precision):
        if pre >= 0.85:
          rec.append(recall[i])
      a = open('grid_search.txt','a+')
      a.write('{0}\t{1}\t{2}\t{3}\n'.format(colsample_bytree,subsample,learning_rate,max(rec)))
      a.close()