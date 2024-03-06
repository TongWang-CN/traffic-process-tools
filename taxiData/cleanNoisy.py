def extractOD(file:str,colname:list,colParameter:list,have_header,timetypeIsStamp=True,printImage=False):
    '''
    返回配送行程、空载行程、od表格
    Parameters:
        colParameter: list类型 按[traId,time,lng,lat,status]顺序排列
    '''
    import pandas as pd
    import numpy as np
    import transbigdata as tbd
    read_path=file
    traId,timename,lng,lat,status=colParameter[0],colParameter[1],colParameter[2],colParameter[3],colParameter[4]

    #读取文件，格式化处理文件
    if have_header:
        df=pd.read_csv(read_path,header=0)
        
    else:
        df=pd.read_csv(read_path,names=colname)
    df.dropna(inplace=True)
    print(df)

    
    #判断时间种类
    if timetypeIsStamp:#时间戳类型
        df['time']=pd.to_datetime(df[timename],unit='s')
        df['time']=df['time'].dt.strftime('%Y/%m/%d %H:%M:%S')
        timename='time'

    df.sort_values(by=[timename,traId])
    print('read finished')
    

    #清洗数据
    dropNoisyDf=tbd.clean_taxi_status(df, col=[traId, timename, status])

    #生成OD矩阵
    odDf=tbd.taxigps_to_od(dropNoisyDf, col=[traId, timename, lng,lat, status])

    #计算OD距离
    odDf['distance']=tbd.getdistance(odDf['slon'], odDf['slat'], odDf['elon'], odDf['elat'])

    #提取轨迹
    deliverDf,emptyDf=tbd.taxigps_traj_point(dropNoisyDf, odDf, col=[traId, timename,lng,lat, status])

    print('extract finished')
    del df
    del dropNoisyDf
    #if printImage:
    
    '''
    #统计轨迹经过的路段数量
    segmentNumber=deliverDf.groupby(by='ID')['osm_fid'].nunique().reset_index()
    segmentNumber.rename({'osm_fid':'segmentNumber'},axis=1,inplace=True)

    #绘图
    segmentNumber=segmentNumber.sort_values(by='segmentNumber')
    segmentNumberSub=segmentNumber[segmentNumber['segmentNumber']<150]

    import matplotlib.pyplot as plt
    from matplotlib.font_manager import FontProperties           
    font = FontProperties(family='Ubuntu')
    fig, ax = plt.subplots(1, 1, figsize=(8, 8), sharex=False)
    plt.grid(True,axis='both') # 添加网格线
    bar_plot = ax.hist(segmentNumberSub['segmentNumber'], label='Mean Day Count', bins=100)
    cumulative_count = np.cumsum(segmentNumberSub['segmentNumber']) / np.sum(segmentNumberSub['segmentNumber'])
    ax2 = ax.twinx()
    line_plot, = ax2.plot(segmentNumberSub['segmentNumber'],cumulative_count, linestyle='-', color='red', label='Cumulative Probability')

    ax.set_title(date, fontproperties=font, fontsize=12)
    ax.set_xlabel('segment number', fontproperties=font, fontsize=10)
    ax.set_ylabel('count', fontproperties=font, fontsize=10)
    ax2.set_ylabel('Cumulative Probability', fontproperties=font, fontsize=10)

    lines, labels = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines + lines2, labels + labels2, loc='right', prop={'size': 8})


    deliver_path=f'/media/wtong/文件/dataSet/chengduTaxi/deliver/{date}.csv'
    od_path=f'/media/wtong/文件/dataSet/chengduTaxi/od/{date}.csv'
    image_path=f'/media/wtong/文件/dataSet/chengduTaxi/image/{date}.png'
    #存储轨迹
    deliverDf.to_csv(deliver_path,index=False)
    #存储OD
    odDf.to_csv(od_path,index=False)
    #存储图片
    plt.savefig(image_path,dpi=120)
    '''
    return deliverDf,emptyDf,odDf
if __name__=="__main__":
    import os
    import pandas as pd
    import numpy as np
    #import transbigdata as tbd
    from multiprocessing import Pool,cpu_count
    from datetime import datetime
    from joblib import Parallel,delayed

    '''
    dir='/media/wtong/文件/dataSet/wuhanTaxi/speedDf/'
    filelist=os.listdir(dir)
    colname=['traId','timestamp','pass1','longitude','latitude','pass2','status']
    colParameter=['traId','timestamp','longitude','latitude','status']
    for filename in filelist:
        print(filename)
        file=dir+filename
        deliverDf,emptyDf,odDf=extractOD(dir+filename,colname,colParameter,have_header=True,timetypeIsStamp=True,printImage=False)

        alldf=pd.concat([deliverDf,emptyDf],axis=0)


        deliverPath='/media/wtong/文件/dataSet/wuhanTaxi/deliver/'
        emptyPath='/media/wtong/文件/dataSet/wuhanTaxi/empty/'
        odPath='/media/wtong/文件/dataSet/wuhanTaxi/od/'
        allPath='/media/wtong/文件/dataSet/wuhanTaxi/allconcat/'

        deliverDf.to_csv(deliverPath+filename,index=False)
        emptyDf.to_csv(emptyPath+filename,index=False)
        odDf.to_csv(odPath+filename,index=False)
        alldf.to_csv(allPath+filename,index=False,sep=';')
        print('finish')
    '''

    

    def processdf(deliverPath,emptyPath,allPath,filename):
        
        print(filename)
        deliverDf=pd.read_csv(deliverPath+filename,header=0)
        emptyDf=pd.read_csv(emptyPath+filename,header=0)

        deliverDf['MID']=deliverDf['ID']
        emptyDf['MID']=emptyDf['ID']+1000000000

        alldf=pd.concat([deliverDf,emptyDf],axis=0)
        alldf['time']=pd.to_datetime(alldf['timestamp'],unit='s')
        alldf['time']=alldf['time'].dt.strftime('%Y/%m/%d %H:%M:%S')
        alldf['time']=pd.to_datetime(alldf['time'],format='%Y/%m/%d %H:%M:%S')
        from datetime import date, timedelta as td
        alldf['time']=alldf['time']+td(hours=8)

        alldf.to_csv(allPath+filename,index=False,sep=';')
        print('finish')

    deliverPath='F:/dataSet/wuhanTaxi/deliver/'
    emptyPath='F:/dataSet/wuhanTaxi/empty/'

    allPath='F:/dataSet/wuhanTaxi/allconcat/'
    filelist=[
               'WH_20180710.csv', 'WH_20180711.csv', 'WH_20180712.csv', 'WH_20180713.csv', 'WH_20180714.csv', 'WH_20180715.csv', 'WH_20180716.csv', 'WH_20180717.csv',
                 'WH_20180718.csv', 'WH_20180719.csv', 'WH_20180720.csv', 'WH_20180721.csv', 'WH_20180722.csv', 'WH_20180723.csv', 'WH_20180724.csv', 'WH_20180725.csv',
                   'WH_20180726.csv', 'WH_20180727.csv', 'WH_20180728.csv', 'WH_20180729.csv', 'WH_20180730.csv', 'WH_20180731.csv', 'WH_20180801.csv', 'WH_20180802.csv',
                     'WH_20180803.csv', 'WH_20180804.csv', 'WH_20180805.csv', 'WH_20180806.csv', 'WH_20180807.csv', 'WH_20180808.csv', 'WH_20180809.csv', 'WH_20180810.csv',
                       'WH_20180811.csv', 'WH_20180812.csv', 'WH_20180813.csv', 'WH_20180814.csv', 'WH_20180815.csv', 'WH_20180816.csv', 'WH_20180817.csv', 'WH_20180818.csv',
                         'WH_20180819.csv', 'WH_20180820.csv', 'WH_20180821.csv', 'WH_20180822.csv', 'WH_20180823.csv', 
              'WH_20180824.csv', 'WH_20180825.csv', 'WH_20180826.csv', 'WH_20180827.csv', 'WH_20180828.csv', 'WH_20180829.csv', 'WH_20180830.csv', 'WH_20180831.csv']
    Parallel(n_jobs=4, prefer="threads")(delayed(processdf)(deliverPath,emptyPath,allPath,filename) for filename in filelist)