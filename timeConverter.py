import pandas as pd
import numpy as np
import time,datetime
import matplotlib.pyplot as plt
import matplotlib
import os
df = pd.read_csv('./geolifedata.csv')

loadedid=[]
startTimes={}
lastTime=0.0

def putStart(id,tstr):
    if not id in startTimes:
        try:
            startTimes[id]=int(time.mktime(time.strptime(tstr, "%Y/%m/%d %H:%M:%S")))
        except Exception:
            startTimes[id] = int(time.mktime(time.strptime(tstr, "%Y-%m-%d %H:%M:%S")))
    return 0

df.apply(lambda x:putStart(x["id"],x["date"]+" "+x["time"]),axis=1)

def getTS(id,tstr):
    try:
        t = int(time.mktime(time.strptime(tstr, "%Y/%m/%d %H:%M:%S")))
    except Exception:
        t = int(time.mktime(time.strptime(tstr, "%Y-%m-%d %H:%M:%S")))
    return t - startTimes[id]

# for row in df.iterrows():
#     attr = row[1]
#     dateString = attr["date"]
#     timeString = attr["time"]
#     x=dateString+" "+timeString
#     t= int(time.mktime(time.strptime(x, "%Y/%m/%d %H:%M:%S")))
#     attr["time"]=t-startTimes[attr["id"]]

df["time"]=df.apply(lambda x:getTS(x["id"],x["date"]+" "+x["time"]),axis=1)

df=df.drop(columns=["date"])


mask = (df["longitude"]<42)&(df["longitude"]>38) & (df["latitude"]<119)&(df["latitude"]>114)
df=df.loc[mask]

df.drop_duplicates(["id","time"],keep="first",inplace=True)

df.to_csv("./output.csv",sep=",",header=True,index=False)



