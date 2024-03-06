def extractOD(dir,file):
    import pandas as pd
    import numpy as np
    import transbigdata as tbd
    read_path=dir+file
    date=file[0:8]
    #读取文件，格式化处理文件
    colname=['traId','latitude','longitude','status','time','timestamp','osm_fid','mx','my']
    df=pd.read_csv(read_path,names=colname)
    df.dropna(inplace=True)
    df.sort_values(by=['time','traId'])
    df['time']=pd.to_datetime(df['time'],format='%Y/%m/%d %H:%M:%S')

    #清洗数据
    dropNoisyDf=tbd.clean_taxi_status(df, col=['traId', 'time', 'status'])

    #生成OD矩阵
    odDf=tbd.taxigps_to_od(dropNoisyDf, col=['traId', 'time', 'mx','my', 'status'])

    #计算OD距离
    odDf['distance']=tbd.getdistance(odDf['slon'], odDf['slat'], odDf['elon'], odDf['elat'])

    #提取轨迹
    deliverDf,emptyDf=tbd.taxigps_traj_point(dropNoisyDf, odDf, col=['traId', 'time', 'mx','my', 'status'])

    del df
    del dropNoisyDf

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


    deliver_path=f'F:/文件/dataSet/chengduTaxi/deliver/{date}.csv'
    od_path=f'F:/文件/dataSet/chengduTaxi/od/{date}.csv'
    image_path=f'F:/文件/dataSet/chengduTaxi/image/{date}.png'
    #存储轨迹
    deliverDf.to_csv(deliver_path,index=False)
    #存储OD
    odDf.to_csv(od_path,index=False)
    #存储图片
    plt.savefig(image_path,dpi=120)

import os


dir='F:/文件/dataSet/chengduTaxi/combine/'
filelist=os.listdir(dir)

for file in filelist:
    extractOD(dir,file)