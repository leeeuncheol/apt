from ast import keyword
import json
import time
from multiprocessing.connection import wait
from bs4 import BeautifulSoup
import requests
import pandas as pd 
import numpy as np 
import datetime as dt
from sqlalchemy import create_engine, true
import pymysql
import os
import webbrowser
from fake_useragent import UserAgent
from retrying import retry
import re


code = pd.read_csv('법정동코드 전체자료_전라도_무안.txt', sep='\t')
code.columns = ['code', 'name', 'is_exist']
filter = code['is_exist'].str.contains("존재")
subset_code = code[filter]
subset_code = subset_code[['name']]
subset_code['z'] = ''
subset_code['lat'] = ''
subset_code['lon'] = ''
subset_code['cortarNo'] = ''
subset_code.reset_index(drop=True, inplace=True)
print(subset_code)


keyword = ''    


for i in range(0, len(subset_code)):
# for i in range(0, 5):

    try:
         keyword = subset_code.iloc[i,0]
    except:
        print('keyword 오류')
        continue
    
    print(keyword)
    

    
    # if (len(keyword.split(' ')) != 3):
    #     if (keyword.split(' ')[1] == '포항시') | (keyword.split(' ')[1] == '창원시'):
    #         if (len(keyword.split(' ')) != 4):
    #             continue
        
    #         if keyword.split(' ')[3].endswith('읍') | keyword.split(' ')[3].endswith('면') | keyword.split(' ')[3].endswith('리') :
    #             continue
    #     else :
    #         continue
    
    try : 
        if (len(keyword.split(' ')) != 2):
            continue
    except : 
        continue

   
    # res = pd.DataFrame()
    ua = UserAgent(use_cache_server=True)

    # keyword = "울산광역시 남구"
    # gubun = 4 

    url = "https://m.land.naver.com/search/result/" + keyword
    try:
        
        headers = {'User-Agent': ua.random,}

        res =requests.get(url, headers=headers)    
        time.sleep(3)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "lxml")


        # print("res.text=", type(res.text), res.text) 
        # print("soup", type(soup), soup)


        # filter내 정보를 가져오기 위해 split을 이용한 수작업 진행
        # re.함수를 이용하여 list로 저장
        filter1 = res.text.split('filter: {', 1)
        filter2 = filter1[1].split('},', 1)
        filter3 = filter2[0].lstrip().rstrip() # 최종 "lat:--, lon:--, ..." 정보만 얻어옴
    except : 
        print('정보수집 불가')
        continue

    # re.escape 함수는 문자열을 입력받으면 특수문자들을 이스케이프 처리시켜 준다.
    # re.compile은 컴파일을 미리 해 두고 이를 저장할 수 있다
    regex = re.compile('{}(.*){}'.format(re.escape("'"), re.escape("'")))
    filter_lists = regex.findall(filter3)
    # print(filter_lists) # ['37.4937', '126.8823', '14', '1153010200', '구로동', '*', 'A1:B1:B2']

    lat = filter_lists[0]
    lon = filter_lists[1]
    z = filter_lists[2]
    cortarNo = filter_lists[3]

    # print(lat,lon,z,cortarNo)
    
    subset_code.at[i, 'z'] = z
    subset_code.at[i, 'lat'] = lat
    subset_code.at[i, 'lon'] = lon
    subset_code.at[i, 'cortarNo'] = cortarNo
    
    
    
print(subset_code)

subset_code.set_index('name',drop=True,inplace=True)

#CSV파일 저장
subset_code.to_csv(os.path.join("check_무안.csv"), index=True,encoding="utf-8") 
    
 