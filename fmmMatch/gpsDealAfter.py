import pandas as pd
import geopandas as gpd
import time
import os
import numpy as np

from multiprocessing import Pool,cpu_count
from datetime import datetime
from joblib import Parallel,delayed



def dealFmm(matchid,disgap_file,fmm_file,road_file,output_file):
    ''''

    matchid:用于匹配的车辆id
    disgap_file:筛选后文件
    fmm_file:fmm匹配文件
    road_file:路网文件
    output_file:处理后文件输出路径
    
    '''


    gps_fmm = pd.read_csv(fmm_file,sep=',',header=0,index_col=None)
    print('read fmmfile')

    
    # 计算匹配路段的字段osm_fid
    df_mroad=gps_fmm .join(gps_fmm['opath'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename('osm_fid'))
    print('road id')

    df_mroad=df_mroad.reset_index(drop=True)
    df_mroad=df_mroad.reset_index()
    df_mroad.sort_values(by=['id','index'],inplace=True)#固定顺序
    df_mroad  = df_mroad.dropna(axis=0, how='any')
    df_mroad=df_mroad[['id','index','osm_fid']]
    df_mroad=df_mroad.reset_index(drop=True)#重置索引
    print('road_id process finised! ')
    

    # 计算校正坐标
    gps_fmm = pd.read_csv(fmm_file,sep=',',header=0,index_col=None)

    df_mpoint=gps_fmm
    df_mpoint['pgeom']=gps_fmm['pgeom'].map(lambda x: str(x)[11:-1])#获取坐标序列
    df_mpoint=df_mpoint.join(df_mpoint['pgeom'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename('mpoint'))#拆分坐标
    df_mpoint=df_mpoint.reset_index(drop=True)
    df_mpoint=df_mpoint.reset_index()
    df_mpoint.sort_values(by=['id','index'],inplace=True)#固定顺序
    df_mpoint  = df_mpoint.dropna(axis=0, how='any')
    df_mpoint=df_mpoint.reset_index(drop=True)
    df_mpoint[['mx','my']]=df_mpoint['mpoint'].str.split(' ', expand=True)
    df_mpoint=df_mpoint[['id','index','mx','my']]
    print('dealing mx,my')

    #取出原始文件中匹配成功的记录
    gps_disgap= pd.read_csv(disgap_file,sep=';',header=0)
    data=gps_disgap[gps_disgap[matchid].isin(df_mroad['id'])]
    data=data.reset_index(drop=True)
    data=data.sort_values(by=[matchid,'timestamp'])
    del gps_disgap
    print('dealing origin')


    #在原始gps文件上添加信息
    gps_roadid=data.join(df_mroad['osm_fid'].astype(int))#添加fid信息
    road_shp=gpd.read_file(road_file)
    roadid_dict=dict(zip(road_shp['fid_1'],road_shp['idnew']))#添加idnew
    gps_roadid['osm_idnew']=gps_roadid['osm_fid'].map(roadid_dict)
    gps_roadid=gps_roadid.join(df_mpoint[['mx','my']])#添加校正后的点信息
    #print(gps_roadid)
    #gps_roadid=gps_roadid.drop([matchid],axis=1)
    #保存文件
    gps_roadid.to_csv(output_file,encoding='utf-8',index=False,header=False,mode='a')
    length=len(gps_roadid)
    print('sub process finished!')
    return gps_roadid,length




def file_read_process(date,matchid,file,road_file,disgap_path,fmm_path,output_path,splitnum):
    '''
    date:日期 格式20180801
    matchid:用于匹配的路网id
    file:筛选后的原始文件名称
    road_file:路网文件
    disgap_path:筛选后文件夹
    fmm_path:匹配后文件文件夹
    output_path:处理完毕的文件的输出文件夹
    splitnum:切分份数

    
    '''

    print(f'deal-----------{date}')
    disgap_file=disgap_path+f'{file}'
    output_file=output_path+f'{date}.csv'
    for i in range(0,splitnum):
        subnum=i+1
        fmmname=f'{date}sub{subnum}.csv'
        fmm_file=fmm_path+fmmname
 

        gps_roadid,length=dealFmm(matchid,disgap_file,fmm_file,road_file,output_file)

        print(f'处理轨迹点{length}个')
        print(f'处理后文件存储于{output_file}')

def splitfile(fmm_folder,filepath,save_file,splitnum,date):
    ''''
    当文件过大时无法直接处理，需要用此函数将匹配文件切分成若干小文件
    fmm_folder:fmm匹配后输出的原始文件夹
    filepath：匹配文件
    save_file：切分后文件保存路径
    date：轨迹日期，格式20180801
    splitnum：单日文件切分份数
    
    '''

    print(filepath)
    gps_fmm = pd.read_csv(fmm_folder+filepath,sep=';',header=0,index_col=None)
    sub_dfs=np.array_split(gps_fmm,splitnum)         
    for i,sub_df in enumerate(sub_dfs):
        sub_df.to_csv(save_file+f'{date}sub{i+1}.csv',index=False)
    del gps_fmm
    del sub_dfs
    return 0



if __name__ == "__main__":

    start=time.time()

    datadir='/media/wtong/文件/matchdata/data/'#文件目录
    disgap_path=datadir+'reTraject/' #匹配前筛选后的gps文件
    fmm_folder=datadir+'matchfile/' #fmm的输出文件
    sub_fmm_path=datadir+'sub/'#保存
    output_path=datadir+'dealmatch/'#输出处理文件保存路径
    road_file=datadir+'road/SimplyfyAll.shp'#路网文件
    splitnum=60


    filelist=os.listdir(disgap_path)#几个日期文件
    
    print('begin split')
    Parallel(n_jobs=4, prefer="threads")(delayed(splitfile)(fmm_folder,fmmfile,sub_fmm_path,splitnum,date=fmmfile[3:-4]) for fmmfile in filelist)
    print('split finished')



    print('begin process')
    Parallel(n_jobs=1, prefer="threads")(delayed(file_read_process)(fmmfile,road_file,disgap_path,sub_fmm_path,output_path,splitnum) for fmmfile in filelist)
    end=time.time()
    processTime=end-start
    print('process finished!! time {}min',processTime/60)



