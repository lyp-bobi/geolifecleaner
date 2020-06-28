import pandas as pd
import numpy as np
import time,datetime
import matplotlib.pyplot as plt
import matplotlib
import os

df=pd.read_csv("./GLSC.csv")

mask=(df["time"]>=7200) &(df["time"]<=10800)
df=df[mask]

df.to_csv("./simp.csv",sep=",",header=True,index=False)
