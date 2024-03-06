import pandas as pd
import numpy as np

speedResult=pd.read_csv('2023_08_01_speed.csv',index_col=0)
barDf=pd.read_csv('resultSPEED.csv',index_col=0)

concatdf=pd.concat([speedResult,barDf],axis=1)

#v实际/v自由   
concatdf.iloc[:,0:-4]=concatdf.iloc[:,0:-4].apply(lambda x: x / concatdf.iloc[:,-4])
#nan值填充为1
concatdf.fillna(1.0,inplace=True)
concatdf.to_csv('congestionIndex.csv')
