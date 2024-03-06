def calGridID(x,y):#计算格网
    import math
    id=math.ceil((x-510618.413100000005215)/1000)*32-math.floor((y-3365999.853999999817461)/1000)
    return id

def format_int(sgrid,egrid):#编码od
    formatValue=f's{sgrid:03d}e{egrid:03d}'
    return formatValue


#定义时间筛选函数
def is_time_in_range(stime,etime):
    #开始时间在范围内
    flag1=(7 <= stime.hour <9) or (17 <= stime.hour <19) 
    #结束时间在范围内
    flag2=(7 <= etime.hour <9) or (17 <= etime.hour <19) 
    flag=flag1 and flag2
    return flag


def filterOD(week,odfile):
    '''
    返回轨迹全集deliverDf
    短中长子集shortDf,mediumDf,longDf
    高峰非高峰子集 rushDf,unRushDf

    '''
    import pandas as pd
    import numpy as np
    import geopandas as gpd


    aveOdPath=f'/media/wtong/文件/dataSet/chengduTaxi/od_file/{week}.shp'
    odPtah=f'/media/wtong/文件/dataSet/chengduTaxi/od/{week}/{odfile}'
    deliverPath=f'/media/wtong/文件/dataSet/chengduTaxi/deliver/{odfile}'

    od=pd.read_csv(odPtah)
    deliverDf=pd.read_csv(deliverPath)
    aveDf=gpd.read_file(aveOdPath)

    #筛选
    deliverDf=deliverDf[deliverDf['flag']==1]
    od=od[od['ID'].isin(deliverDf['ID'])]
    
    print(od)
    print(deliverDf)



    '''
    处理轨迹
    '''
    #统计路段数量
    segmentNumber=deliverDf.groupby(by='ID')['osm_fid'].nunique().reset_index()
    segmentNumber.rename({'osm_fid':'segmentNumber'},axis=1,inplace=True)
    segmentNumber=segmentNumber[segmentNumber['segmentNumber']>=5]
    #获取路段数量在5及以上的轨迹
    print(segmentNumber)
    deliverDf=deliverDf[deliverDf['ID'].isin(segmentNumber['ID'])]




    '''
    处理OD
    '''


    #根据期终点表简单筛选
    od['etime']=pd.to_datetime(od['etime'],format='%Y-%m-%d %H:%M:%S')
    od['stime']=pd.to_datetime(od['stime'],format='%Y-%m-%d %H:%M:%S')
    od['timegap']=(od['etime']-od['stime']).dt.total_seconds()
    od.sort_values(by='timegap',inplace=True)
    od['speed']=(od['distance']/od['timegap'])*3.6
    od=od[od['distance']>0]#删除不动点
    od=od[(od['speed']>=10)&(od['speed']<=120)]#简单筛选速度过慢或者超速的数据
    od['sgrid']=od.apply(lambda row: calGridID(row['slon'],row['slat']) ,axis=1)
    od['egrid']=od.apply(lambda row: calGridID(row['elon'],row['elat']) ,axis=1)
    od['odCode']=od.apply(lambda row: format_int(row['sgrid'],row['egrid']),axis=1)#OD格网编码

    #获取流量多的od格网对
    aveDf=aveDf[aveDf['count']>=20]
    aveDf['odCode']=aveDf.apply(lambda row: format_int(int(row['id_x']),int(row['id_y'])),axis=1)#OD格网编码
    #获取在格网对里的的od记录
    od=od[od['odCode'].isin(aveDf['odCode'])]
    
    #获取流量速度筛选后的轨迹
    deliverDf=deliverDf[deliverDf['ID'].isin(od['ID'])]



    '''
    划分子集
    '''
    #短、中、长
    shortod=od[od['distance']<5000]
    mediumod=od[(od['distance']>=5000)&(od['distance']<10000)]
    longod=od[(od['distance']>=10000)]
    
    shortDf=deliverDf[deliverDf['ID'].isin(shortod['ID'])]
    mediumDf=deliverDf[deliverDf['ID'].isin(mediumod['ID'])]
    longDf=deliverDf[deliverDf['ID'].isin(longod['ID'])]
    print('split df into short,medium,long')

    #高峰7-9,17-19
    rushod=od[od.apply(lambda row:is_time_in_range(row['stime'],row['etime']),axis=1 )]
    unrushod=od[~od['ID'].isin(rushod['ID'])]

    rushDf=deliverDf[deliverDf['ID'].isin(rushod['ID'])]
    unRushDf=deliverDf[deliverDf['ID'].isin(unrushod['ID'])]


    return deliverDf,shortDf,mediumDf,longDf,rushDf,unRushDf

import os

week='weekend'
dir=f'/media/wtong/文件/dataSet/chengduTaxi/od/{week}/'
filelist=os.listdir(dir)

for file in filelist:
    filterodf=filterOD(week,file)
    filterodf.to_csv(f'/media/wtong/文件/dataSet/chengduTaxi/filterod/{file}')

