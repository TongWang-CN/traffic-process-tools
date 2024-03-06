import pandas as pd
import numpy as np
import geopandas as gpd
import transbigdata as tbd
import time
import os

from multiprocessing import Pool,cpu_count
from datetime import datetime
from joblib import Parallel,delayed



def clean_gps(road_file,name):
    print(name)

    gps_file = f'/media/wtong/文件/matchdata/data/reTraject/{name}'
    output_file=f'/media/wtong/文件/dataSet/wuhanTaxi/calspeed/filter/{name}'#筛选后的点
    #计算路网shp坐标范围
    road_shp = gpd.GeoDataFrame.from_file(road_file, encoding='utf-8')
    bounds=road_shp.total_bounds.tolist()
    minx,miny,maxx,maxy=bounds[0],bounds[1],bounds[2],bounds[3]
    print(minx,miny,maxx,maxy)

    #选出范围内的点
    #namecol=['traId','speed1','speed2','pass','angle','time','longitude','latitude']
    #gps_df=pd.read_csv(gps_file,header=0,names=namecol,sep=';')
    #gps_df['timestamp']=pd.to_datetime(gps_df['time']).astype(int)//10**9
    #0,1000001,1530560363.0,11288.0,524811.889471448,3381340.51190157,1.0,1.0,51.221708310228586
    
    #colmame=['indexcol','traId','timestamp','pass1','longitude','latitude','pass2','status','speed']
    gps_df=pd.read_csv(gps_file,header=0,index_col=None,sep=';')
    #gps_df.drop(['indexcol','pass1'],inplace=True, axis=1)
    gps_df['time']=pd.to_datetime(gps_df['timestamp'],unit='s')
    gps_df['time']=gps_df['time'].dt.strftime('%Y/%m/%d %H:%M:%S')
    #print(gps_df)
    #采样频率
    #gps_df['timestamp']=pd.to_datetime(gps_df['time'],format='%Y/%m/%d %H:%M:%S').astype(int)//10**9
    inputLength=len(gps_df)
    gps_inbound=gps_df[((gps_df['longitude']>=minx)&(gps_df['longitude']<=maxx))&
                    ((gps_df['latitude']>=miny)&(gps_df['latitude']<=maxy))]
    print(gps_inbound)
    
    # 前后间隔100m重新划分轨迹,为轨迹重新编码，存储为新文件
    gps_inbound= gps_inbound.sort_values(by=['traId','time'])
    gps_disgap=tbd.id_reindex(gps_inbound,col='traId',timegap=10,suffix='_new')
    gps_disgap.to_csv(output_file,index=False,sep=';')
    outputLength=len(gps_disgap)
    portion=round(outputLength/inputLength,3)
    print(f'输入点数量{inputLength},输出点数量{outputLength},比例{portion}')
    print(f'筛选文件存储于{output_file}')
    return gps_disgap,inputLength,outputLength,portion

if __name__ == "__main__":


    start_time=time.time()
    #input
    road_file='/media/wtong/文件/matchdata/data/CGCSroad/CGCSmatch.shp' #路网
    #gps_file = '../data/gps.csv' # 原始点数据
    gps_folder='/media/wtong/文件/dataSet/wuhanTaxi/calspeed/speedDF/'
    gps_list=os.listdir(gps_folder)
    #clean_gps(road_file,'WH_20180702.csv')
    Parallel(n_jobs=1, prefer="threads")(delayed(clean_gps)(road_file,name) for name in gps_list)
    end_time=time.time()
    elapsed_time=round(end_time-start_time,3)
    print(f'筛选用时{elapsed_time}s')
    
    '''
    for name in gps_list:
        #output
        gps_file = f'/media/wtong/文件/Amap/processdata/sortcsv/{name}'
        output_file=f'/media/wtong/文件/matchdata/data/reTraject/{name}'#筛选后的点
        gps_disgap,inputLength,outputLength,portion=clean_gps(road_file,gps_file,output_file)


        end_time=time.time()
        elapsed_time=round(end_time-start_time,3)
        print(f'筛选用时{elapsed_time}s,输入点数量{inputLength},输出点数量{outputLength},比例{portion}')
        print(f'筛选文件存储于{output_file}')
    '''