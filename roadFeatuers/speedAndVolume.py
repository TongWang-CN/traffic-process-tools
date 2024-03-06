import arcpy
from arcpy import env
import pandas as pd
import numpy as np
import concurrent.futures



from simpledbf import Dbf5
from tqdm import tqdm
from datetime import datetime
import os, time, sys


from multiprocessing import Pool,cpu_count
from joblib import Parallel,delayed


# 允许覆盖同名文件
#设置工作空间
arcpy.env.overwriteOutput = True
env.workspace = "E:/Amap/processdata/count0704"
dir = "E:/Amap/processdata/count0704/"


# 导入XY坐标数据
def createxyfromtable(in_Table, x_coords, y_coords, out_Layer, point_shp):
    # 输入：csv数据，x,y坐标，输出的shp文件
    # Make the XY event layer...
    arcpy.MakeXYEventLayer_management(in_Table, x_coords, y_coords, out_Layer)
    # Process: Copy Features
    # arcpy.CopyFeatures_management(out_Layer, point_shp, "", "0", "0", "0")
    arcpy.CopyFeatures_management(out_Layer, point_shp)

#harmonic_mean = np.harmonic_mean(data)

def spe_volume_count(filename,n):
    cols=['id','speed1','speed2','pass','angle','time','lng','lat']
    #df = pd.read_csv('E:/Amap/processdata/dealcsv/' + filename,names=cols,header=0)

    #格式化名称
    year,month,day = filename[:4],filename[4:6],filename[6:8]
    date = f"{year}-{month}-{day}"
    date_name=f"{year}{month}{day}"

    #使用高峰期做测试
    time_list = pd.date_range(date + ' 20:00:00', date + ' 21:59:59', freq='10min')
    #最后一个23：55：00
    print(time_list)
    # 构造流量计算矩阵 2567个路段
    nums = [i for i in range(n)]
    volume_df = pd.DataFrame(data=nums,columns=["ind"])
    speed_df = pd.DataFrame(data=nums,columns=["ind"])
    gpsPonit_df = pd.DataFrame(data=nums,columns=["ind"])
    # 从00：00开始每隔5min计算
    for i in tqdm(range(0,len(time_list))):
        # 开始时间结束时间 5min为间隔
        start_time = str(time_list[i])
        if i == len(time_list)-1:   # 到最后一个了
            end_time = date+" 22:00:00"
        else:
            end_time = str(time_list[i+1])

        # 获取索引 the first and the last 
        print(start_time,end_time)
        start_name = year+month+day+start_time[11:13]+start_time[14:16]
        #首尾index
        '''
        start = df[(df["time"]>= start_time) & (df["time"]<end_time)].index[0]
        end = df[(df["time"]>= start_time) & (df["time"]<end_time)].index[-1]

        #开始时间名称07010830  8位
        start_name = year+month+day+start_time[11:13]+start_time[14:16]#时、分
        
        df.iloc[start:end + 1, :].to_csv(dir +'datetimeseg/'+ start_name + ".csv")#存储文件，按时间切分
        # CSV文件转点SHP 格式化得到文件
        createxyfromtable(dir +'datetimeseg/'+ start_name + ".csv", "lng", "lat", f"P{start_name}c", dir+f'mapdata/P{start_name}c.shp')
        
        # 调用NEAR工具箱 做临近,使用线程锁
 
        #lock.acquire()
        arcpy.Near_analysis(dir+f'mapdata/P{start_name}c.shp', "E:\\Amap\\processdata\\result\\processroad\\map12.shp", "20 Meters", "LOCATION", "NO_ANGLE", "GEODESIC")
        #lock.release()
        # 计算流量
        '''
        
        RAW_DBF = Dbf5(dir+f'mapdata/P{start_name}c.dbf')
        RAW_DF = RAW_DBF.to_dataframe()
        #读取一个 DBF 文件，并将其转换为一个 pandas DataFrame


        #计算流量
        vol = RAW_DF.groupby('NEAR_FID')['id'].nunique().reset_index()[['NEAR_FID', 'id']]
        vol = vol[vol['NEAR_FID'] >=0]
        vol.columns = ['NEAR_FID', 'volume_t']
        vol_nums = [0] * n
        for j in range(len(vol)):
            #每行对应
            vol_nums[vol.iloc[j, 0]] = vol.iloc[j, 1]
        #增加一列
        volume_df[start_name] = vol_nums


        #计算速度
        
        spe = RAW_DF.groupby('NEAR_FID')['speed1'].mean().reset_index()[['NEAR_FID', 'speed1']]
        spe = spe[spe['NEAR_FID'] >= 0]
        spe.columns = ['NEAR_FID', 'roadspeed']
        speed_nums = [0] * n
        for k in range(len(spe)):
            #列对应的位置
            speed_nums[spe.iloc[k, 0]] = spe.iloc[k, 1]
        #增加一列
        #speed_nums = np.array(speed_nums).astype(dtype=int).tolist()
        speed_df[start_name] = speed_nums

        #计算轨迹点采样
        gpsnum = RAW_DF.groupby(['NEAR_FID']).count().reset_index()[['NEAR_FID', 'speed1']]
        gpsnum = gpsnum[gpsnum['NEAR_FID'] >= 0]
        gpsnum.columns = ['NEAR_FID', 'gpscount']
        gps_nums = [0] * n
        for j in range(len(gpsnum)):
            #每行对应
            gps_nums[gpsnum.iloc[j, 0]] = gpsnum.iloc[j, 1]
        #增加一列
        gpsPonit_df[start_name] = gps_nums
        

    volume_df = volume_df.drop(columns=['ind'])
    volume_df.to_csv(dir+f"volume/vol{date_name}.csv")

    speed_df = speed_df.drop(columns=['ind'])
    speed_df.to_csv(dir+f"speed/speed{date_name}.csv")

    gpsPonit_df = gpsPonit_df.drop(columns=['ind'])
    gpsPonit_df.to_csv(dir+f"gps/gpsnum{date_name}.csv")
    del volume_df
    del speed_df
    #del df
    del gpsPonit_df
if __name__=="__main__":
    #读取所有文件目录
    

    csv_files = os.listdir('E:/Amap/processdata/dealcsv')
    num_cores = cpu_count()
    print(f'cpu count------{num_cores}')
    #Parallel(n_jobs=num_cores, prefer="threads")(delayed(spe_volume_count)(filename, 2567) for filename in csv_files)
    #改线程数量
    Parallel(n_jobs=6, prefer="threads")(delayed(spe_volume_count)(filename, 10161) for filename in csv_files)

    

    print('finish')

