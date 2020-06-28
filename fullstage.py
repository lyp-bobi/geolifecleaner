import pandas as pd
import numpy as np
import time,datetime
import matplotlib.pyplot as plt
import matplotlib
import os
df = pd.read_csv('./tdrive.csv')

def getTS(tstr):
    try:
        t = int(time.mktime(time.strptime(tstr, "%Y/%m/%d %H:%M:%S")))
    except Exception:
        t = int(time.mktime(time.strptime(tstr, "%Y-%m-%d %H:%M:%S")))
    return t


df.loc[:,"time"]=df.apply(lambda x:getTS(x["datetime"]),axis=1)

df=df.loc[:,["id","time","x","y"]]

df.to_csv("output.csv",sep=",",header=True,index=False)
# df=df.drop(columns=["date"])
#
# dfPrev=df.shift(periods=1,axis=0)
# dfPrev.drop(dfPrev.tail(1).index)
# df["prevId"]=dfPrev["id"]
# df["prevTime"]=dfPrev["time"]
#
# global newid
# global curid
#
# starttimes={}
# newid=df["id"].max()+1
# curid=0
#
# def getId(id1,id2,time1,time2):
#     global newid
#     global curid
#     if id1 not in starttimes:
#         starttimes[id1]=(time1//86400)*86400
#         curid=id1
#     elif id1==id2 and (time1-time2>18000 or time1-starttimes[curid]>86400):
#         curid=newid
#         newid+=1
#         starttimes[curid] = (time1//86400)*86400
#     if time1>88000 and starttimes[curid]<86400:
#         starttimes[curid]+=86400
#     return curid
#
# df.loc[:,('id')]=df.apply(lambda x:getId(x["id"],x["prevId"],x["time"],x["prevTime"]),axis=1)
#
# df=df.drop(columns=["prevId","prevTime"])
#
# df.loc[:,"time"]=df.apply(lambda x:x["time"]-starttimes[x["id"]],axis=1)
#
# df.to_csv("./output.csv",sep=",",header=True,index=False)