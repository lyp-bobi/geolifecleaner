import pandas as pd
import numpy as np
import time,datetime
import matplotlib.pyplot as plt
import matplotlib
import os
df = pd.read_csv('./GLS.csv')
mask = (df["longitude"]<42)&(df["longitude"]>38) & (df["latitude"]<119)&(df["latitude"]>114)
df=df.loc[mask]

# df.drop_duplicates(["id","time"],keep="first",inplace=True)

df = pd.DataFrame(df.groupby(['id', 'time']).mean().reset_index())
df=df.loc[:,["id","longitude","latitude","time"]]
df.to_csv("./output.csv",sep=",",header=True,index=False)