import pandas as pd
import math
import matplotlib.pyplot as plt
df=pd.read_csv("TD.csv")

dfPrev=df.shift(periods=1,axis=0)
dfPrev.drop(dfPrev.tail(1).index)

df["lastid"]=dfPrev["id"]
df["lastx"]=dfPrev["x"]
df["lasty"]=dfPrev["y"]
df["lasttime"]=dfPrev["time"]
# df.loc[:,["lastid","lastx","lasty","lasttime"]]=dfPrev.loc[:,["id","x","y","time"]]

def speed(id,x,y,t,pid,px,py,pt):
    if(id!=pid):
        return 0
    return math.sqrt((px-x)*(px-x)+(py-y)*(py-y))/(t-pt)

df.loc[:,"speed"]=df.apply(lambda x:speed(x["id"],x["x"],x["y"],x["time"],x["lastid"],x["lastx"],x["lasty"],x["lasttime"]),axis=1)

df["speed"].hist(bins=100)

plt.show()