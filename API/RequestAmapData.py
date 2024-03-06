import requests
import pandas as pd
import json
import time




def get_data(url,x,times):
    data=requests.get(url)
    s=data.json()
    x.append([times,s])
    return x


def get_amap_trafficinfo(baselng,baselat,level):

    widthlng=0.04
    #同一维度，lng=0.01≈1000米
    widthlat=0.03

    save_path='E:/Amap/requestAmap/download/downLevel'+str(level)+'.csv'
    url='https://restapi.amap.com/v3/traffic/status/rectangle?key=f1cd698c4d87b51b7f4722a6328eef7b&extensions=all&rectangle='
    times = time.strftime('%Y-%m-%d,%H:%M', time.localtime())

    num = 0
    x = []
    #循环每个网格进行数据爬取，在这里构建了3X3网格
    for i in range(0,3):
        #设定网格单元的左下与右上坐标的纬度值
        #在这里对数据进行处理，使之保留6位小数（不保留可能会莫名其妙出错）
        startlat=round(baselat+i*widthlat,6)
        endlat=round(startlat+widthlat,6)
        for j in range(0,2):
            #设定网格单元的左下与右上坐标的经度值
            startlng=round(baselng+j*widthlng,6)
            endlng=round(startlng+widthlng,6)
            #设置API的URL并进行输出测试
            locStr=str(startlng)+","+str(startlat)+";"+str(endlng)+","+str(endlat)

            thisUrl=url+locStr

            print(thisUrl)

            getData=get_data(thisUrl,x,time)

    df = pd.DataFrame(getData, columns=['time', 'all'])

    df.to_csv(save_path, mode='a', header=False, encoding='utf-8-sig')


baselng=114.138137
baselat=30.471475
level=5

get_amap_trafficinfo(baselng,baselat,level)
    
    
'''if __name__=="__main__": 

    while True:
    #爬取过程可能会出错中断，因此增加异常处理
        try:
            #输入左下
            baselng=114.138137
            baselat=30.471475
            level=5

            get_amap_trafficinfo(baselng,baselat,level)

            time.sleep(300)
        except Exception as e:
            print('error!'*10)
            continue'''