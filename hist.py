import pandas as pd
import matplotlib.pyplot as plt

df=pd.read_csv("./GLSC.csv")

id=df.groupby("id").count()

len=id.groupby("time").count()

len["longitude"].hist(bins=50)

plt.show()

time=df.groupby("id")
period=time.max()-time.min()
periodcount=period.groupby("time").count()
