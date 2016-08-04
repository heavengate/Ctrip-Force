#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import division
import pandas as pd
import numpy as np
import xgboost as xgb
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# dat_path = 'force06.txt'
dat_path = 'force07.txt'
rawdat = pd.read_table(dat_path, sep='\t', header=0, na_values='NULL')
#rawdat.dtypes
#rawdat.shape

#最大房量为0的改为NA，否则相除会成Inf
rawdat.loc[rawdat.htl_max_rmqty==0,'htl_max_rmqty'] = np.nan
rawdat.loc[rawdat.mbr_max_rmqty==0,'mbr_max_rmqty'] = np.nan
rawdat.loc[rawdat.mhtl_max_rmqty==0,'mhtl_max_rmqty'] = np.nan

#母酒店房间量默认用room_quantity，异常用最大房量填充
rawdat.loc[rawdat.mhtl_roomquantity.isnull(),'mhtl_roomquantity'] = rawdat.mhtl_max_rmqty
rawdat.loc[rawdat.mhtl_roomquantity==0,'mhtl_roomquantity'] = rawdat.mhtl_max_rmqty
rawdat.loc[rawdat.mhtl_roomquantity==9999,'mhtl_roomquantity'] = rawdat.mhtl_max_rmqty

nominator_features = [
  'htl_succeed_rmqty_efdt30d',
  'mbr_succeed_rmqty_efdt30d',
  'mhtl_succeed_rmqty_efdt30d',
  'htl_force_succeed_rmqty_efdt30d',
  'mbr_force_succeed_rmqty_efdt30d',
  'mhtl_force_succeed_rmqty_efdt30d',

  'htl_full_ordnum_efdt30d',
  'mbr_full_ordnum_efdt30d',
  'mhtl_full_ordnum_efdt30d',
  'htl_force_full_ordnum_efdt30d',
  'mbr_force_full_ordnum_efdt30d',
  'mhtl_force_full_ordnum_efdt30d',

  'htl_full_ordnum_30d',
  'mbr_full_ordnum_30d',
  'mhtl_full_ordnum_30d',
  'htl_force_full_ordnum_30d',
  'mbr_force_full_ordnum_30d',
  'mhtl_force_full_ordnum_30d',
]
rawdat[nominator_features] = rawdat[nominator_features].fillna(0)

rawdat['htl_rmqty_occupied'] = rawdat.htl_succeed_rmqty_efdt30d / rawdat.htl_max_rmqty
rawdat['mbr_rmqty_occupied'] = rawdat.mbr_succeed_rmqty_efdt30d / rawdat.mbr_max_rmqty
rawdat['mhtl_rmqty_occupied'] = rawdat.mhtl_succeed_rmqty_efdt30d / rawdat.mhtl_roomquantity
rawdat['htl_force_rmqty_pct'] = rawdat.htl_force_succeed_rmqty_efdt30d / rawdat.htl_succeed_rmqty_efdt30d
rawdat['mbr_force_rmqty_pct'] = rawdat.mbr_force_succeed_rmqty_efdt30d / rawdat.mbr_succeed_rmqty_efdt30d
rawdat['mhtl_force_rmqty_pct'] = rawdat.mhtl_force_succeed_rmqty_efdt30d / rawdat.mhtl_succeed_rmqty_efdt30d

#满房率
rawdat['htl_fullpct_efdt30d'] = rawdat.htl_full_ordnum_efdt30d / rawdat.htl_ordnum_efdt30d
rawdat['mbr_fullpct_efdt30d'] = rawdat.mbr_full_ordnum_efdt30d / rawdat.mbr_ordnum_efdt30d
rawdat['mhtl_fullpct_efdt30d'] = rawdat.mhtl_full_ordnum_efdt30d / rawdat.mhtl_ordnum_efdt30d
rawdat['htl_force_fullpct_efdt30d'] = rawdat.htl_force_full_ordnum_efdt30d / rawdat.htl_force_ordnum_efdt30d
rawdat['mbr_force_fullpct_efdt30d'] = rawdat.mbr_force_full_ordnum_efdt30d / rawdat.mbr_force_ordnum_efdt30d
rawdat['mhtl_force_fullpct_efdt30d'] = rawdat.mhtl_force_full_ordnum_efdt30d / rawdat.mhtl_force_ordnum_efdt30d

rawdat['htl_fullpct_30d'] = rawdat.htl_full_ordnum_30d / rawdat.htl_ordnum_30d
rawdat['mbr_fullpct_30d'] = rawdat.mbr_full_ordnum_30d / rawdat.mbr_ordnum_30d
rawdat['mhtl_fullpct_30d'] = rawdat.mhtl_full_ordnum_30d / rawdat.mhtl_ordnum_30d
rawdat['htl_force_fullpct_30d'] = rawdat.htl_force_full_ordnum_30d / rawdat.htl_force_ordnum_30d
rawdat['mbr_force_fullpct_30d'] = rawdat.mbr_force_full_ordnum_30d / rawdat.mbr_force_ordnum_30d
rawdat['mhtl_force_fullpct_30d'] = rawdat.mhtl_force_full_ordnum_30d / rawdat.mhtl_force_ordnum_30d

#如果百分比超过1，置为1
rawdat.loc[rawdat.htl_rmqty_occupied>=1,'htl_rmqty_occupied'] = 1
rawdat.loc[rawdat.mbr_rmqty_occupied>=1,'mbr_rmqty_occupied'] = 1
rawdat.loc[rawdat.mhtl_rmqty_occupied>=1,'mhtl_rmqty_occupied'] = 1


rawdat['mhtl_holdpct'] = rawdat.mhtl_hold_ordnum / rawdat.mhtl_ordnum_30d
rawdat.loc[rawdat.defrecommend<=0,'defrecommend'] = 0
rawdat.loc[rawdat.defrecommend>=10,'defrecommend'] = 0


#距离节假日的天数
rawdat['ordd'] = pd.to_datetime(rawdat['ordd'], format='%Y-%m-%d')
rawdat['holidaydt'] = np.zeros((rawdat.shape[0],))
temp1 = rawdat['ordd'] < pd.to_datetime('2016-04-02',format='%Y-%m-%d')
rawdat.loc[temp1,'holidaydt'] = ((pd.to_datetime('2016-04-02',format='%Y-%m-%d')-rawdat.loc[temp1,'ordd'])/np.timedelta64(1,'D')).astype(int)
temp1 = (rawdat['ordd'] > pd.to_datetime('2016-04-04',format='%Y-%m-%d')) & (rawdat['ordd'] < pd.to_datetime('2016-04-30',format='%Y-%m-%d'))
rawdat.loc[temp1,'holidaydt'] = ((pd.to_datetime('2016-04-30',format='%Y-%m-%d')-rawdat.loc[temp1,'ordd'])/np.timedelta64(1,'D')).astype(int)
temp1 = rawdat['ordd'] > pd.to_datetime('2016-05-02',format='%Y-%m-%d')
rawdat.loc[temp1,'holidaydt'] = ((pd.to_datetime('2016-06-09',format='%Y-%m-%d')-rawdat.loc[temp1,'ordd'])/np.timedelta64(1,'D')).astype(int)

rawdat['isnoroom'] = 1 - rawdat['isnoroom']

#只看当天预订
#rawdat = rawdat.loc[rawdat.ordadvanceday2==0,]

#rawdat['ord_hour'].replace([0,1,2,3,4,5,6],1,inplace=True)
#rawdat = rawdat.loc[~(rawdat.eid.isnull()),:]

# nonnumberic_columns = ['hotelbelongto', 'eid_source']
# direct_features = ["ordd","isnoroom",
#                 "mbr_fullpct_30d",
#                 "mbr_force_fullpct_30d",
#                 "mhtl_fullpct_30d",
#                 "mhtl_force_fullpct_30d",
#                 "mbr_fullpct_efdt30d",
#                 "mhtl_fullpct_efdt30d",
#                 "mbr_force_fullpct_efdt30d",
#                 "mbr_force_rmqty_pct",
#                 "mhtl_force_fullpct_efdt30d",
#                 "mhtl_force_rmqty_pct",
#                 "mbr_rmqty_occupied",
#                 "mhtl_rmqty_occupied",
#                 "cityi","zonei","zonestari"]
# whiten_feature = ["mbr_ordnum_30d",
#               "mbr_force_ordnum_30d",
#               "mhtl_ordnum_30d",
#               "mhtl_force_ordnum_30d",
#               "mbr_ordnum_efdt30d",
#               "mhtl_ordnum_efdt30d",
#               "mbr_force_ordnum_efdt30d",
#               "mhtl_force_ordnum_efdt30d"]
# one_hot_features = ['dayofweek','star','goldstar','defrecommend']

# from sklearn.preprocessing import LabelEncoder
# le = LabelEncoder()
# for feature in nonnumberic_columns:
#   rawdat[feature] = le.fit_transform(rawdat[feature])
rawdat["close_hour_per"] = rawdat["closehours_avg"] / rawdat["closetimes_avg"]

# Xdat1 = rawdat[["ordd","isnoroom",
#                 "mbr_fullpct_30d",
#                 "mbr_force_fullpct_30d",
#                 "mhtl_fullpct_30d",
#                 "mhtl_force_fullpct_30d",
#                 "mbr_fullpct_efdt30d",
#                 "mhtl_fullpct_efdt30d",
#                 "mbr_force_fullpct_efdt30d",
#                 "mbr_force_rmqty_pct",
#                 "mhtl_force_fullpct_efdt30d",
#                 "mhtl_force_rmqty_pct",
#                 "mbr_rmqty_occupied",
#                 "mhtl_rmqty_occupied",
#                 "defrecommend",
#                 "cityi","zonei","zonestari"]].copy()
Xdat1 = rawdat[["ordd","isnoroom",
                "mbr_fullpct_30d",
                "mbr_force_fullpct_30d",
                "mhtl_fullpct_30d",
                "mhtl_force_fullpct_30d",
                "mbr_fullpct_efdt30d",
                "mhtl_fullpct_efdt30d",
                "mbr_force_fullpct_efdt30d",
                "mbr_force_rmqty_pct",
                "mhtl_force_fullpct_efdt30d",
                "mhtl_force_rmqty_pct",
                "mbr_rmqty_occupied",
                "mhtl_rmqty_occupied",
                "cityi","zonei","zonestari",
                "defrecommend",]].copy()
# 'mbroom_median_priceratio_efdt7d',
#                 'mbroom_htl_median_priceratio_efdt7d',
#                 'mbr_force_efdt_ratio',
#sklearn.preprocessing.StandardScaler不接受NULL，手动标准化

Xdat2 = rawdat[[ 
              "mbr_ordnum_30d",
              "mbr_force_ordnum_30d",
              "mhtl_ordnum_30d",
              "mhtl_force_ordnum_30d",
              "mbr_ordnum_efdt30d",
              "mhtl_ordnum_efdt30d",
              "mbr_force_ordnum_efdt30d",
              "mhtl_force_ordnum_efdt30d",
              # "closetimes_avg",
              # "closehours_avg",
              "close_hour_per",
              ]].copy()


def XScaler(X): # 均值归零，方差归一化
    mu = np.nanmean(X,axis=0) # nan不计算，其余的求平均，axis标示计算轴
    sigma = np.nanstd(X,axis=0)
    Xscl = (X-mu)/sigma
    return Xscl

Xdat2_scl = XScaler(Xdat2)
del Xdat2

### 分类变量one hot encode
#把字符串转为数值编码
from sklearn.preprocessing import LabelEncoder
le = LabelEncoder()
nonnumberic_columns = ['eid_source', "hotelbelongto"]
for feature in nonnumberic_columns:
  rawdat[feature] = le.fit_transform(rawdat[feature])

from sklearn.preprocessing import OneHotEncoder
one_hot_features = ['dayofweek','star','goldstar'] + nonnumberic_columns
one_hot_var = rawdat[one_hot_features].copy()
enc = OneHotEncoder(sparse=False)
Xenc_2 = enc.fit_transform(one_hot_var)
Xenc_2 = pd.DataFrame(Xenc_2,index=rawdat.index)

Xdat = pd.merge(Xdat1,Xdat2_scl,how='left',left_index=True,right_index=True)
Xdat = pd.merge(Xdat,Xenc_2,how='left',left_index=True,right_index=True)

# Xdat.to_csv('dataset.csv')
# print 'save Xdat to csv'

#训练集测试集
idx_train = (Xdat.ordd>='2016-03-01') & (Xdat.ordd<='2016-04-30')
idx_test = (Xdat.ordd>='2016-05-01') & (Xdat.ordd<='2016-05-15')

# Xdat.drop('ordd',axis=1,inplace=False).to_csv('dataset.csv')

Xtrain = Xdat.loc[idx_train,:]
ytrain = Xdat.loc[idx_train,'isnoroom']
# ytrain = pd.DataFrame(ytrain)
Xtrain = Xtrain.drop('ordd',axis=1,inplace=False)
Xtrain = Xtrain.drop('isnoroom',axis=1,inplace=False)

Xtest = Xdat.loc[idx_test,:]
ytest = Xdat.loc[idx_test,'isnoroom']
# ytest = pd.DataFrame(ytest)
Xtest = Xtest.drop('ordd',axis=1,inplace=False)
Xtest = Xtest.drop('isnoroom',axis=1,inplace=False)

# print Xtrain.shape
# print ytrain.shape
# Xtrain.to_csv('Xtrain.csv')
# ytrain.to_csv('ytrain.csv')
# Xtest.to_csv('Xtest.csv')
# ytest.to_csv('ytest.csv')

# xgb.XGBClassifier
params = {'max_depth':4, 'learning_rate':0.1, 'n_estimators':100,
        'objective':'binary:logistic', 'subsample': 0.5, 'colsample_bytree':1,
        'missing':np.nan}
clfXGB = xgb.XGBClassifier(**params)
clfXGB.fit(Xtrain.fillna(-999).as_matrix(),ytrain.as_matrix(),early_stopping_rounds=30,
        eval_set=[(np.array(Xtest.fillna(-999)),ytest)],
        eval_metric = 'auc', verbose = True)

# clfXGB.save_model('xgb_force.model')
# clfXGB.dump_model('xgb.raw.txt','feamap.txt')

#预测概率
pred_prob = clfXGB.predict_proba(Xtest)

#P-R曲线
from sklearn.metrics import precision_recall_curve
precision,recall,threshold = precision_recall_curve(ytest,pred_prob[:,1])
plt.figure()
plt.plot(recall,precision)
plt.grid(True,ls = '--',which = 'both')
plt.xlabel="recall"
plt.ylabel="percision"
plt.savefig('test_pr.png')

# prlist = {}

# prlist['precision'] = precision
# prlist['recall'] = recall
# prlist['threshold'] = np.append(threshold,1)

# prlist = pd.DataFrame(prlist,columns=['threshold','precision','recall'])
# print prlist.query('recall>=0.5&recall<0.51')

from sklearn.metrics import classification_report, confusion_matrix
from sklearn.metrics import roc_curve, roc_auc_score
y_pred = clfXGB.predict(Xtest)
print (classification_report(ytest,y_pred))
print (confusion_matrix(ytest,y_pred))
fpr,tpr,thresholds = roc_curve(ytest,pred_prob[:,1])
rec = []
for i,pre in enumerate(precision):
  if pre >= 0.85:
    rec.append(recall[i])
print "0.85 percision -> recall: {}".format(max(rec))
plt.figure()
plt.plot(fpr,tpr,label = "The auc of modes is %0.4f"%(roc_auc_score(ytest,y_pred)))
plt.plot([0,1],[0,1],'--')
plt.legend(loc = "lower right")
plt.savefig('test_roc.png')


# pred_prob = clfXGB.predict_proba(Xtrain)

# #P-R曲线
# from sklearn.metrics import precision_recall_curve
# precision,recall,threshold = precision_recall_curve(ytrain,pred_prob[:,1])
# plt.figure()
# plt.plot(recall,precision)
# plt.grid(True,ls = '--',which = 'both')
# plt.xlabel="recall"
# plt.ylabel="percision"
# plt.savefig('train_pr.png')

# from sklearn.metrics import classification_report, confusion_matrix
# from sklearn.metrics import roc_curve, roc_auc_score
# y_pred = clfXGB.predict(Xtrain)
# print (classification_report(ytrain,y_pred))
# print (confusion_matrix(ytrain,y_pred))
# fpr,tpr,thresholds = roc_curve(ytrain,pred_prob[:,1])
# plt.figure()
# plt.plot(fpr,tpr,label = "The auc of modes is %0.4f"%(roc_auc_score(ytrain,y_pred)))
# plt.plot([0,1],[0,1],'--')
# plt.legend(loc = "lower right")
# plt.savefig('train_roc.png')

#变量重要性
# xgb.plot_importance(clfXGB)
