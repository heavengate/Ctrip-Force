#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import division
import pandas as pd
import numpy as np
import xgboost as xgb
import matplotlib.pyplot as plt
from sklearn.metrics import precision_recall_curve

dat_path = 'force17.txt'
rawdat = pd.read_table(dat_path, sep='\t', header=0, na_values='NULL')
rawdat['isbookable'] = 1-rawdat['isnoroom']


#酒店房间量为0、9999和NULL的用30天最大房量填充
rawdat.loc[rawdat.mhtl_roomquantity.isnull(),'mhtl_roomquantity'] = rawdat.mhtl_roomnum_max_30d
rawdat.loc[(rawdat.mhtl_roomquantity==0)|(rawdat.mhtl_roomquantity==9999),'mhtl_roomquantity'] = rawdat.mhtl_roomnum_max_30d

#酒店房间量<30天最大房量的，用30天最大房量填充
rawdat.loc[rawdat.mhtl_roomquantity<rawdat.mhtl_roomnum_max_30d,'mhtl_roomquantity'] = rawdat.mhtl_roomnum_max_30d

rawdat['htl_nocancelroomnum_efdt_30d'].replace(np.nan,0,inplace=True)
rawdat['mbroom_nocancelroomnum_efdt_30d'].replace(np.nan,0,inplace=True)
rawdat['mhtl_nocancelroomnum_efdt_30d'].replace(np.nan,0,inplace=True)
#已预订的房量占比(房量没有0)
rawdat['htl_rmqty_occupied'] = rawdat.htl_nocancelroomnum_efdt_30d / rawdat.htl_roomnum_max_30d
rawdat['mbr_rmqty_occupied'] = rawdat.mbroom_nocancelroomnum_efdt_30d / rawdat.mbroom_roomnum_max_30d
rawdat['mhtl_rmqty_occupied'] = rawdat.mhtl_nocancelroomnum_efdt_30d / rawdat.mhtl_roomquantity

#如果百分比超过1，置为1
rawdat.loc[rawdat.htl_rmqty_occupied>=1,'htl_rmqty_occupied'] = 1
rawdat.loc[rawdat.mbr_rmqty_occupied>=1,'mbr_rmqty_occupied'] = 1
rawdat.loc[rawdat.mhtl_rmqty_occupied>=1,'mhtl_rmqty_occupied'] = 1


rawdat['htl_submitordnum_noroom_efdt_30d'].replace(np.nan,0,inplace=True)
rawdat['mbroom_submitordnum_noroom_efdt_30d'].replace(np.nan,0,inplace=True)
rawdat['mhtl_submitordnum_noroom_efdt_30d'].replace(np.nan,0,inplace=True)
rawdat['htl_submitordnum_noroom_30d'].replace(np.nan,0,inplace=True)
rawdat['mbroom_submitordnum_noroom_30d'].replace(np.nan,0,inplace=True)
rawdat['mhtl_submitordnum_noroom_30d'].replace(np.nan,0,inplace=True)
#满房率
rawdat['htl_fullpct_efdt30d'] = rawdat.htl_submitordnum_noroom_efdt_30d / rawdat.htl_submitordnum_efdt_30d
rawdat['mbr_fullpct_efdt30d'] = rawdat.mbroom_submitordnum_noroom_efdt_30d / rawdat.mbroom_submitordnum_efdt_30d
rawdat['mhtl_fullpct_efdt30d'] = rawdat.mhtl_submitordnum_noroom_efdt_30d / rawdat.mhtl_submitordnum_efdt_30d

rawdat['htl_fullpct_30d'] = rawdat.htl_submitordnum_noroom_30d / rawdat.htl_submitordnum_30d
rawdat['mbr_fullpct_30d'] = rawdat.mbroom_submitordnum_noroom_30d / rawdat.mbroom_submitordnum_30d
rawdat['mhtl_fullpct_30d'] = rawdat.mhtl_submitordnum_noroom_30d / rawdat.mhtl_submitordnum_30d


#强下订单量为0的改为nan，防止出现Inf (强下订单量会有0，非强下没有)
# rawdat['mhtl_force_submitordnum_30d'].replace(0,np.nan,inplace=True)
# rawdat['mbroom_force_submitordnum_30d'].replace(0,np.nan,inplace=True)
# rawdat['htl_force_submitordnum_30d'].replace(0,np.nan,inplace=True)

rawdat['htl_force_submitordnum_noroom_30d'].replace(np.nan,0,inplace=True)
rawdat['mbroom_force_submitordnum_noroom_30d'].replace(np.nan,0,inplace=True)
rawdat['mhtl_force_submitordnum_noroom_30d'].replace(np.nan,0,inplace=True)
#强下满房率
rawdat['htl_force_fullpct_30d'] = rawdat.htl_force_submitordnum_noroom_30d / rawdat.htl_force_submitordnum_30d
rawdat['mbr_force_fullpct_30d'] = rawdat.mbroom_force_submitordnum_noroom_30d / rawdat.mbroom_force_submitordnum_30d
rawdat['mhtl_force_fullpct_30d'] = rawdat.mhtl_force_submitordnum_noroom_30d / rawdat.mhtl_force_submitordnum_30d


#关房的房型占比
#同样先防止出现Inf
rawdat['room_cnt_hpphtl'] = rawdat['room_cnt_hpp'] + rawdat['room_cnt_htl']
# rawdat['room_cnt_hpphtl'].replace(0,np.nan,inplace=True)
# rawdat['room_cnt_elongqunar'].replace(0,np.nan,inplace=True)
#rawdat['room_cnt_sht'].replace(0,np.nan,inplace=True)

rawdat['hpphtl_close_pct'] = (rawdat.close_roomcnt_hpp + rawdat.close_roomcnt_htl) / rawdat.room_cnt_hpphtl
rawdat['elongqunar_close_pct'] = rawdat.close_roomcnt_elongqunar / rawdat.room_cnt_elongqunar
#rawdat['sht_close_pct'] = rawdat.close_roomcnt_sht / rawdat.room_cnt_sht


#距离节假日的天数
rawdat['effectdate'] = pd.to_datetime(rawdat['effectdate'], format='%Y-%m-%d')
rawdat['holidaydt'] = np.zeros((rawdat.shape[0],))
temp1 = rawdat['effectdate'] < '2016-04-02'
rawdat.loc[temp1,'holidaydt'] = ((pd.to_datetime('2016-04-02',format='%Y-%m-%d')-rawdat.loc[temp1,'effectdate'])/np.timedelta64(1,'D')).astype('int64')
temp1 = (rawdat['effectdate'] > '2016-04-04') & (rawdat['effectdate'] < '2016-04-30')
rawdat.loc[temp1,'holidaydt'] = ((pd.to_datetime('2016-04-30',format='%Y-%m-%d')-rawdat.loc[temp1,'effectdate'])/np.timedelta64(1,'D')).astype('int64')
temp1 = rawdat['effectdate'] > '2016-05-02'
rawdat.loc[temp1,'holidaydt'] = ((pd.to_datetime('2016-06-09',format='%Y-%m-%d')-rawdat.loc[temp1,'effectdate'])/np.timedelta64(1,'D')).astype('int64')


#其他变量
rawdat['mhtl_holdpct'] = rawdat.mhtl_holdroom_ordnum_30d / rawdat.mhtl_submitordnum_30d
rawdat.loc[(rawdat.defrecommend<=0)|(rawdat.defrecommend>=10),'defrecommend'] = 0
rawdat['isweekend'] = np.zeros((rawdat.shape[0],))
rawdat.loc[(rawdat.dayofweek==5)|(rawdat.dayofweek==6),'isweekend'] = 1

#看每一列的NULL值
# rawdat.isnull().sum(axis = 0)/rawdat.shape[0]
rawdat['closehours_30d'].replace(np.nan,0,inplace=True)
rawdat['closehours_efdt_30d'].replace(np.nan,0,inplace=True)
rawdat['close_hour_per_time_30d'] = rawdat.closehours_30d / rawdat.closetimes_30d
rawdat['close_hour_per_time_efdt_30d'] = rawdat.closehours_efdt_30d / rawdat.closetimes_efdt_30d
rawdat['noshownum'].replace(np.nan,0,inplace=True)
rawdat['noshowratio'] = rawdat['noshownum'] / rawdat['roomefdtnum']
# rawdat['fromcitycenter'].replace(np.nan,-999.0,inplace=True)
# rawdat['holdnum'].replace(np.nan,0,inplace=True)
# rawdat['noroomnum'].replace(np.nan,0,inplace=True)

# # variance chang test
# rawdat['mhtl_force_fullpct_30d'] = rawdat['mhtl_force_fullpct_30d'] ** 2
# rawdat['htl_force_fullpct_30d'] = rawdat['htl_force_fullpct_30d'] ** 2
# rawdat['mbr_force_fullpct_30d'] = rawdat['mbr_force_fullpct_30d'] ** 2
# rawdat['mhtl_fullpct_30d'] = rawdat['mhtl_fullpct_30d'] ** 2

# rawdat['success_pct'] = 1.0 / rawdat['success_pct']
# rawdat['mhtl_fullpct_efdt30d'] = 1.0 / rawdat['mhtl_fullpct_efdt30d']
# rawdat['elongqunar_close_pct'] = 1.0 / rawdat['elongqunar_close_pct']
# rawdat['htl_fullpct_efdt30d'] = 1.0 / rawdat['htl_fullpct_efdt30d']

# rawdat['noroom_ordnum_room_30d'].replace(np.nan,0,inplace=True)
# rawdat['force_noroom_ordnum_room_30d'].replace(np.nan,0,inplace=True)
# rawdat['ordnum_room_30d'].replace(0,np.nan,inplace=True)
# rawdat['force_ordnum_room_30d'].replace(0,np.nan,inplace=True)
# rawdat['noroom_ordpct_room_30d'] = rawdat['noroom_ordnum_room_30d'] / rawdat['ordnum_room_30d']
# rawdat['force_noroom_ordpct_room_30d'] = rawdat['force_noroom_ordnum_room_30d'] / rawdat['force_ordnum_room_30d']

Xdat = rawdat[["ordd","isbookable",
                  # "ord_hour",
                  "holidaydt",
                  "htl_fullpct_30d",
                  "mbr_fullpct_30d",
                  "mhtl_fullpct_30d",
                  "htl_fullpct_efdt30d",
                  "mbr_fullpct_efdt30d",
                  "mhtl_fullpct_efdt30d",
                  "htl_rmqty_occupied",
                  "mbr_rmqty_occupied",
                  "mhtl_rmqty_occupied",
                  "cityi","zonei","zonestari",
                  "remainder_saletime",
                  # "capp",
                  "timelag_last_close",
                  "mhtl_holdpct",
                  "hpphtl_close_pct",
                  "elongqunar_close_pct",
                  #"sht_close_pct",
                  # "htl_force_fullpct_30d",
                  # "mbr_force_fullpct_30d",
                  # "mhtl_force_fullpct_30d",
                  "htl_submitordnum_30d",
                  "mbroom_submitordnum_30d",
                  "mhtl_submitordnum_30d",
                  "htl_submitordnum_efdt_30d",
                  "mbroom_submitordnum_efdt_30d",
                  "mhtl_submitordnum_efdt_30d",
                  # "htl_force_submitordnum_30d",
                  # "mbroom_force_submitordnum_30d",
                  # "mhtl_force_submitordnum_30d",
                  "close_hour_per_time_30d",
                  "closetimes_30d",
                  # "success_pct",
                  # "noroom_ordnum",
                  # "total_ordnum",
                  "noshownum",
                  "noshowratio",
                  # "holdnum",
                  # "noroomnum",
                  # "fromcitycenter",
                  # "fromairport",
                  # "fromrailway",
                  # "noroom_ordnum_room_30d",
                  # "force_noroom_ordnum_room_30d",
                  # "noroom_ordpct_room_30d",
                  # "force_noroom_ordpct_room_30d",
                  # "mhtlnum",
                  # "guarantee",
                ]].copy()


### 分类变量one hot encode
for i in ['star','goldstar','defrecommend','isweekend','capp','ord_hour']:
  dummies = pd.get_dummies(rawdat[i],prefix=i)
  for j in range(dummies.shape[1]):
    dummies.iloc[:,j] = dummies.iloc[:,j].astype('int64')
  Xdat = Xdat.join(dummies)

# for col in Xdat.columns:
#   print col, ': ', 1.0 - sum(Xdat[col].notnull()) / float(len(Xdat))

# Xdat.to_csv('Xdat.csv')

#train test set
idx_train = (Xdat.ordd>='2016-04-01') & (Xdat.ordd<'2016-04-24')
idx_validation = (Xdat.ordd>='2016-04-24') & (Xdat.ordd<'2016-05-01')
idx_test = (Xdat.ordd>='2016-05-01') & (Xdat.ordd<='2016-05-15')

ytrain = Xdat.loc[idx_train,'isbookable']
Xtrain = Xdat.loc[idx_train,:]
Xtrain = Xtrain.drop(['ordd','isbookable'],axis=1)

yvalidation = Xdat.loc[idx_validation,'isbookable']
Xvalidation = Xdat.loc[idx_validation,:]
Xvalidation = Xvalidation.drop(['ordd','isbookable'],axis=1)

ytest = Xdat.loc[idx_test,'isbookable']
Xtest = Xdat.loc[idx_test,:]
Xtest = Xtest.drop(['ordd','isbookable'],axis=1)

# Xtrain.to_csv('Xtrain.csv')
# ytrain.to_csv('ytrain.csv')
# Xtest.to_csv('Xtest.csv')
# ytest.to_csv('ytest.csv')
# print "save to csv"

#xgb.train
dtrain = xgb.DMatrix(Xtrain.as_matrix(),label=ytrain,missing=np.nan)
dvalidation = xgb.DMatrix(Xvalidation.as_matrix(),label=yvalidation,missing=np.nan)
dtest = xgb.DMatrix(Xtest.as_matrix(),label=ytest,missing=np.nan)

def unrecall_error(preds, dtrain):
  precision,recall,threshold = precision_recall_curve(dtrain.get_label(),preds)
  return 'unrecall(0.85 precison)', 1.0 - recall[precision>=0.85].max()

def f1_error(preds,dtrain):
  label = dtrain.get_label()
  pred = [int(i >= 0.5) for i in preds]
  tp = sum([int(i==1 and j==1) for i,j in zip(pred,label)])
  precision = float(tp)/sum(pred)
  recall = float(tp)/sum(label)

  return 'f1-score', -1 * (precision * recall / (precision + recall))

# params = {'booster':'gbtree', 'max_depth':5, 'learning_rate':0.1, 'objective':'binary:logistic',
#           'subsample':0.8, 'colsample_bytree':1, 'min_child_weight':10, 'reg_alpha':1,'eval_metric':'auc'}
params = {'booster':'gbtree', 'max_depth':5, 'learning_rate':0.1, 'objective':'binary:logistic',
          'subsample':0.8, 'colsample_bytree':1, 'min_child_weight':10, 'eval_metric':'auc'}

bst = xgb.train(params, dtrain, num_boost_round=1000, evals=[(dvalidation,'eval')], early_stopping_rounds=30)
#bst.best_ntree_limit,bst.best_score

preds = bst.predict(dtest)
precision,recall,threshold = precision_recall_curve(ytest,preds)
print "0.85 precesion -> recall: ", recall[precision>=0.85].max()


# preds = bst.predict(dtest,ntree_limit=bst.best_ntree_limit)

# precision,recall,threshold = precision_recall_curve(ytest,preds)

# prlist = {}

# prlist['precision'] = precision
# prlist['recall'] = recall
# prlist['threshold'] = np.append(threshold,1)

# prlist = pd.DataFrame(prlist,columns=['threshold','precision','recall'])
