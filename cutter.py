import pandas as pd
import numpy as np
import time,datetime
import matplotlib.pyplot as plt
import matplotlib
import os
df = pd.read_csv('./GLCD1.csv')
# df=pd.read_csv("./small.csv")

dfPrev=df.shift(periods=1,axis=0)
dfPrev.drop(dfPrev.tail(1).index)
print(dfPrev)
df["prevId"]=dfPrev["id"]
df["prevTime"]=dfPrev["time"]
print(df)

starttimes={}

global newid
global curid

newid=df["id"].max()+1
curid=0
print("newid is "+str(newid))

def getId(id1,id2,time1,time2):
    global newid
    global curid
    if id1 not in starttimes:
        starttimes[id1]=time1
        curid=id1
    elif id1==id2 and time1-time2>7200:
        curid=newid
        newid+=1
        starttimes[curid] = time1
    return curid

df.loc[:,('id')]=df.apply(lambda x:getId(x["id"],x["prevId"],x["time"],x["prevTime"]),axis=1)
print(df)
df.loc[:,("time")]=df.apply(lambda x:(x["time"]-starttimes[x["id"]]),axis=1)
df=df.drop(columns=["prevId","prevTime"])
df.to_csv("./try1try.csv",sep=",",header=True,index=False)