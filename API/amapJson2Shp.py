# -- coding: utf-8 --**
import pandas as pd
import numpy as np
import time
import json
import hashlib
from urllib import parse
import math
import geopandas as gpd
from shapely.geometry import LineString,Point


#坐标转换
class CoordinateConverter:
    def __init__(self):
        self.x_pi = 3.14159265358979324 * 3000.0 / 180.0
        self.pi = 3.1415926535897932384626  # π
        self.a = 6378245.0  # 长半轴
        self.ee = 0.00669342162296594323  # 偏心率平方

    def bd09_to_gcj02(self, bd_lon, bd_lat):
        """
        百度坐标系(BD-09)转火星坐标系(GCJ-02)
        百度——>谷歌、高德
        :param bd_lat:百度坐标纬度
        :param bd_lon:百度坐标经度
        :return:转换后的坐标列表形式
        """
        x = bd_lon - 0.0065
        y = bd_lat - 0.006
        z = math.sqrt(x * x + y * y) - 0.00002 * math.sin(y * self.x_pi)
        theta = math.atan2(y, x) - 0.000003 * math.cos(x * self.x_pi)
        gg_lng = z * math.cos(theta)
        gg_lat = z * math.sin(theta)
        return [gg_lng, gg_lat]

    def gcj02_to_wgs84(self, lng, lat):
        """
        GCJ02(火星坐标系)转GPS84
        :param lng:火星坐标系的经度
        :param lat:火星坐标系纬度
        :return:
        """
        if self.out_of_china(lng, lat):
            return [lng, lat]
        dlat = self._transformlat(lng - 105.0, lat - 35.0)
        dlng = self._transformlng(lng - 105.0, lat - 35.0)
        radlat = lat / 180.0 * self.pi
        magic = math.sin(radlat)
        magic = 1 - self.ee * magic * magic
        sqrtmagic = math.sqrt(magic)
        dlat = (dlat * 180.0) / ((self.a * (1 - self.ee)) / (magic * sqrtmagic) * self.pi)
        dlng = (dlng * 180.0) / (self.a / sqrtmagic * math.cos(radlat) * self.pi)
        mglat = lat + dlat
        mglng = lng + dlng
        return [lng * 2 - mglng, lat * 2 - mglat]

    def bd09_to_wgs84(self, bd_lon, bd_lat):
        lon, lat = self.bd09_to_gcj02(bd_lon, bd_lat)
        return self.gcj02_to_wgs84(lon, lat)

    def gcj02_to_bd09(self, lng, lat):
        """
        火星坐标系(GCJ-02)转百度坐标系(BD-09)
        谷歌、高德——>百度
        :param lng:火星坐标经度
        :param lat:火星坐标纬度
        :return:
        """
        z = math.sqrt(lng * lng + lat * lat) + 0.00002 * math.sin(lat * self.x_pi)
        theta = math.atan2(lat, lng) + 0.000003 * math.cos(lng * self.x_pi)
        bd_lng = z * math.cos(theta) + 0.0065
        bd_lat = z * math.sin(theta) + 0.006
        return [bd_lng, bd_lat]

    def wgs84_to_gcj02(self, lng, lat):
        """
        WGS84转GCJ02(火星坐标系)
        :param lng:WGS84坐标系的经度
        :param lat:WGS84坐标系的纬度
        :return:
        """
        if self.out_of_china(lng, lat):
            return [lng, lat]
        dlat = self._transformlat(lng - 105.0, lat - 35.0)
        dlng = self._transformlng(lng - 105.0, lat - 35.0)
        radlat = lat / 180.0 * self.pi
        magic = math.sin(radlat)
        magic = 1 - self.ee * magic * magic
        sqrtmagic = math.sqrt(magic)
        dlat = (dlat * 180.0) / ((self.a * (1 - self.ee)) / (magic * sqrtmagic) * self.pi)
        dlng = (dlng * 180.0) / (self.a / sqrtmagic * math.cos(radlat) * self.pi)
        mglat = lat + dlat
        mglng = lng + dlng
        return [mglng, mglat]

    def wgs84_to_bd09(self, lon, lat):
        lon, lat = self.wgs84_to_gcj02(lon, lat)
        return self.gcj02_to_bd09(lon, lat)

    def out_of_china(self, lng, lat):
        """
        判断是否在国内，不在国内不做偏移
        :param lng:
        :param lat:
        :return:
        """
        return not (lng > 73.66 and lng < 135.05 and lat > 3.86 and lat < 53.55)

    def _transformlng(self, lng, lat):
        ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + \
            0.1 * lng * lat + 0.1 * math.sqrt(math.fabs(lng))
        ret += (20.0 * math.sin(6.0 * lng * self.pi) + 20.0 *
                math.sin(2.0 * lng * self.pi)) * 2.0 / 3.0
        ret += (20.0 * math.sin(lng * self.pi) + 40.0 *
                math.sin(lng / 3.0 * self.pi)) * 2.0 / 3.0
        ret += (150.0 * math.sin(lng / 12.0 * self.pi) + 300.0 *
                math.sin(lng / 30.0 * self.pi)) * 2.0 / 3.0
        return ret

    def _transformlat(self, lng, lat):
        ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + \
            0.1 * lng * lat + 0.2 * math.sqrt(math.fabs(lng))
        ret += (20.0 * math.sin(6.0 * lng * self.pi) + 20.0 *
                math.sin(2.0 * lng * self.pi)) * 2.0 / 3.0
        ret += (20.0 * math.sin(lat * self.pi) + 40.0 *
                math.sin(lat / 3.0 * self.pi)) * 2.0 / 3.0
        ret += (160.0 * math.sin(lat / 12.0 * self.pi) + 320 *
                math.sin(lat * self.pi / 30.0)) * 2.0 / 3.0
        return ret


#提取数据
def extract_func(read_data):
    all_list=[]
    num=0
    error_count=0
    print('————————————————开始提取信息——————————————')
    #提取信息
    for line in range(0,len(read_data)):
        print('处理第>>>>>'+str(line)+'条记录')
        #获取时间
        times=read_data.iloc[line]['time']
        #获取信息
        info=read_data.iloc[line]['info']
        #单引号转双引号解析json
        info= info.replace("'", '"')
        info = json.loads(info)

        roads_info=info['trafficinfo']['roads']
        #读每条路
        for road in range(0,len(roads_info)):

            mat=[times,roads_info[road].get('name'),roads_info[road].get('status'),
                 roads_info[road].get('speed'),roads_info[road].get('lcodes'),roads_info[road].get('polyline')]
            if mat not in all_list:
                all_list.append(mat)
                num+=1
                #print(num)
    trans_df = pd.DataFrame(all_list, columns=['time', 'name', 'status', 'speed','lcodes','polyline'])
    new_data=trans_df.copy()
    new_data['name_code']=new_data['name']+','+new_data['lcodes']
    #返回提取后的数据
    print('————————————信息提取结束————————————————')
    return new_data

#清洗数据
def time_stamp(data_path):
    
    downLoadDir='E:/Amap/requestAmap/download/'
    read_path=downLoadDir+data_path

    cols=['grid','time','info']
    read_data=pd.read_csv(read_path,header=None,encoding='utf-8-sig',names=cols)
    read_data=read_data.dropna()
    extract_df=extract_func(read_data)


    extract_df['time_stamp']=extract_df['time'].apply(
        lambda x: time.strptime(x, '%Y-%m-%d,%H:%M')
    )
    extract_df['time_stamp']=extract_df['time_stamp'].apply(
        lambda x: time.mktime(x)
    )


    extract_df.sort_values(by=['time','name'], ascending=True, inplace=True)
    extract_df.drop_duplicates(keep='first',inplace=True)
    extract_df['time_diff']=extract_df['time_stamp'].diff(periods=1)

    #name=data_path.split(',')
    #name=name[0]
    saveDir='E:/Amap/requestAmap/result/extractDf/'
    save_path=saveDir+f'ExDf{data_path}.csv'
    extract_df.to_csv(save_path,encoding='utf-8-sig')
    print(f'提取后的记录保存至文件夹：{save_path}')
    return extract_df


def split_func(data_path):
    #返回一个数据文件
    data_df=time_stamp(data_path)
    m=data_df['polyline'].str.replace(';',',')
    coor_df=m.str.split(',',expand=True)
    coor_df.astype(float)
    col_num=coor_df.shape[1]
    split_df=pd.concat([data_df,coor_df],axis=1)
    return split_df,col_num

#114.432838,30.5919113;114.433083
#trans data to geo_df
def data_to_shp(data_path):
    #split_df.loc[6888,'time_stru']
    #调用自定义函数返回切分后的文件
    split_df,col_num=split_func(data_path)
    print('——————————————开始路段坐标转线————————————————')
    #属性
    features=[]
    #地理信息
    geomList=[]
    for line in range(0,len(split_df)):
        #存储本路坐标
        LineList=[]
        #分离属性
        features.append(split_df.iloc[line,1:9])
        #打包同一组的点
        for co in range(0,col_num,2):
            point_t=[0.0,0.0]
            lng=split_df.loc[line,co]
            lat=split_df.loc[line,co+1]
            #如果是nan 跳出内层循环
            if pd.isna(lng):
                print('本路段统计结束,count>>>'+str(line))
                break
            else:
                #转换坐标为WGS84
                lng=float(lng)
                lat=float(lat)
                #在这里加一个坐标转换
                transcoor=CoordinateConverter()
                mglng,mglat=transcoor.gcj02_to_wgs84(lng,lat)
                point_t[0]=mglng
                point_t[1]=mglat
                add_p=tuple(point_t)
                LineList.append(add_p)
        line_list=LineString(LineList)
        geomList.append(line_list)

    geomDataFrame=gpd.GeoDataFrame(features,geometry=geomList)
    return geomDataFrame
    print('————————————路段坐标转线结束————————————————')

#从表格
def json2shp(split_label,data_path):
    saveDir='E:/Amap/requestAmap/result/splitShp/'
    print('————————————开始切分图像————————————————')
    read_df=data_to_shp(data_path)
    diff_label=list(read_df[split_label].unique())
    group_df=read_df.groupby(split_label)
    for label in diff_label:
        groupFrame=group_df.get_group(label)
        print('process group>>>'+str(label))
        idty=str(int(label))
        groupFrame.to_file(saveDir+'s'+idty+'.shp',driver='ESRI Shapefile',encoding='utf-8')
    print('————————————切分结束————————————————')
    
json2shp('time_stamp','level.csv')
'''
if __name__=="__main__": 

    start=time.time()
    #读取文件
    save_path=r'C:/Users/MI/Documents/study/Amap/20230315project/data/shp/20230320/WGS84/'
    #转为火星坐标图形文件
    
    split_label='time_stamp'
    shp_name='L62_20230320'
    shp=data_to_shp(62)
    shp.to_file(save_path+shp_name+'.shp',driver='ESRI Shapefile',encoding='utf-8')
    frame_group_split(split_label,shp,save_path)
    
    end=time.time()
    time_use=end-start
    print('finish,time cost>>>>'+str(time_use)+'s')'''
