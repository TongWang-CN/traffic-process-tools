import pandas as pd
#import geopandas as gpd
import time
import numpy as np
import os
from multiprocessing import Pool,cpu_count
from datetime import datetime
from joblib import Parallel,delayed





def splitfile(fmm_folder,filepath,save_file):
    print(filepath)

    gps_fmm = pd.read_csv(fmm_folder+filepath,sep=';',header=0,index_col=None)
    sub_dfs=np.array_split(gps_fmm,100)         
    for i,sub_df in enumerate(sub_dfs):
        sub_df.to_csv(save_file+f'{filepath[0:8]}sub{i+1}.csv',index=False)
    del gps_fmm
    del sub_dfs
    return 0

fmm_folder='/media/wtong/文件/matchdata/data/matchfile/' #fmm的输出
filelist=os.listdir(fmm_folder)
save_file='/media/wtong/文件/matchdata/data/sub/'#保存
Parallel(n_jobs=2, prefer="threads")(delayed(splitfile)(fmm_folder,filepath,save_file) for filepath in filelist)