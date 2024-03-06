#清洗源数据
import pandas as pd
import numpy as np
import os
import time
from datetime import datetime
import json

#读取文件
def data_clean(read_path):
    read_dir='dealcsv/'
    save_dir='sortcsv/'

    cols=['id','speed1','speed2','pass','angle','time','lng','lat']
    data=pd.read_csv(read_dir+read_path,header=None,names=cols,index_col=None,low_memory=False)
    data = data.drop_duplicates()
    data.sort_values(by='time',inplace=True,ascending=True)
    data.dropna()
    #save_name=str(2018)+read_path.split('_')[-2]  + file_name.split('_')[-1].split('.')[0]

    data.to_csv(save_dir+read_path,header=True,index=False,sep=';')
    return 0


if __name__=="__main__":
    #读取所有文件目录
    csv_files = os.listdir('dealcsv/')
    for file in csv_files:
        # 提取文件名
        file_name = os.path.basename(file)


        print (file_name)
        # 使用文件名提取日期
        save_name=data_clean(file_name)
        print(save_name)
    print('finish')