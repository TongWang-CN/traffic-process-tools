from geographiclib.geodesic import Geodesic
import pandas as pd

from multiprocessing import Pool,cpu_count
from datetime import datetime
from joblib import Parallel,delayed
import os, time, sys
import numpy as np



class speedCalClass():
    def __init__(self,colParameters) -> None:
        self.colParameters=colParameters
    def extract_azi1(row):
        geodesic_result = Geodesic.WGS84.Inverse(row['lat'],row['lng'],row['lat2'] , row['lng2'])
        azi=geodesic_result['azi1']
        if azi>=0:
            return azi
        elif azi<0:
            return azi+360
        else:
            return np.nan
        
    def extract_s12(row):
        geodesic_result = Geodesic.WGS84.Inverse(row['lat'],row['lng'],row['lat2'] , row['lng2'])
        length=geodesic_result['s12']
        return length

    def lengCalculate(self,row):
        lng,lat=self.colParameters[0],self.colParameters[1]
        length=((row[lat]-row['lat2'])**2+ (row['lng2']-row[lng])**2)**0.5
        return length



    def AngleNewDf(self,dir,date):
        filelist=os.listdir(dir+date)
        count=0
        for splitfile in filelist:
            read_path=dir+date+'/'+splitfile
            df = pd.read_csv( read_path, index_col=None, header=0)
            df['lng2'] = df['lng'].shift(-1)#向上偏移
            df['lat2'] = df['lat'].shift(-1)
            df.fillna(0,inplace=True)

            #df['Calangle'] = df.apply(extract_azi1, axis=1)
            df['l2']=df.apply(self.lengCalculate, axis=1)
            df['timediff']=df['timestamp'].diff(1).shift(-1)
            df.fillna(0,inplace=True)


            df['speed2']=df.apply(lambda x: (x['l2']/x['timediff'])*3.6
                                if x['timediff']!=0 else 0, axis=1)
            
            df['speed1']=df['speed2'].shift(1)#向下位移
            df['l1']=df['l2'].shift(1)
            df.fillna(0,inplace=True)
            df['l']=df['l1']+df['l2']
            #速度加权平均
            df['speed']=(df['speed1']*df['l1']+df['speed2']*df['l2'])/df['l']
            df.drop(['lng2','lat2','speed2','speed1','l1','l','l2','timediff'], axis=1, inplace=True)#删除多余列
            print

            #df.to_csv('E:\\Amap\\processdata\\AngleSplitCarCsv\\' + date + '\\' + filename)
            # 合并文件
            df.to_csv(dir + date + '.csv', mode='a', header=False, index=True)
            count+=1
            print(f"Processed {date} file  {count}")

        return 0


    def speedCalfun(self,df):
        #print(df)
        

        lng,lat,timestamp=self.colParameters[0],self.colParameters[1],self.colParameters[2]
        df=df.sort_values(by=timestamp)
        df['lng2'] = df[lng].shift(-1)#向上偏移
        df['lat2'] = df[lat].shift(-1)
        df.fillna(0,inplace=True)
        #print(df)

        #df['Calangle'] = df.apply(extract_azi1, axis=1)
        df['l2']=df.apply(self.lengCalculate, axis=1)
        df['timediff']=df[timestamp].diff(1).shift(-1)
        df.fillna(0,inplace=True)


        df['speed2']=df.apply(lambda x: (x['l2']/x['timediff'])*3.6
                            if x['timediff']!=0 else 0, axis=1)
        
        df['speed1']=df['speed2'].shift(1)#向下位移
        df['l1']=df['l2'].shift(1)
        df.fillna(0,inplace=True)
        df['l']=df['l1']+df['l2']
        #速度加权平均
        df['speed']=(df['speed1']*df['l1']+df['speed2']*df['l2'])/df['l']
        df.drop(['lng2','lat2','speed2','speed1','l1','l','l2','timediff'], axis=1, inplace=True)#删除多余列
        return df 



if __name__=="__main__":
    #切分文件中的所有子文件夹
    
    
     
    #本段是处理所有文件的遍历
    start=time.time()
    


    def process(read_path,save_path,file,speedCal,col):
        print(file)

        df=pd.read_csv(read_path+file,names=col)
        df.sort_values(by=['traId','timestamp'])
        df['timestamp']=df['timestamp'].astype('int')
        

        #应用
        new=df.groupby('traId').apply(speedCal.speedCalfun)
        new=new.reset_index(drop=True)
        new.to_csv(save_path+file,index=False)
        return 0
    
    read_path='/media/wtong/文件/dataSet/wuhanTaxi/wait/'
    save_path='/media/wtong/文件/dataSet/wuhanTaxi/speedDf/'

    col=['traId','timestamp','pass1','longitude','latitude','pass2','status']
    colParameters=['longitude','latitude','timestamp']
    speedCal=speedCalClass(colParameters)
    fatherfile=os.listdir(read_path)
    Parallel(n_jobs=4)(delayed(process)(read_path,save_path,file,speedCal,col) for file in fatherfile)

    end=time.time()
    print(end-start)



