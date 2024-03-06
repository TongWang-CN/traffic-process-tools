import pandas as pd
import os


dir='D:/dataSet/chengduTaxi/combine/'
filelist=os.listdir(dir)
col=['traId','latitude','longitude','status','time','timestamp','osm_fid','mx','my']
df = pd.read_csv('D:/dataSet/chengduTaxi/result/20140818.csv')
print(df)
#for file in filelist:
    #df = pd.read_csv(dir+file, on_bad_lines='skip')
    #df.to_csv(f'D:/dataSet/chengduTaxi/result/{file}',index=False)