from ast import keyword
import json
import time
from multiprocessing.connection import wait
from bs4 import BeautifulSoup
import requests
import pandas as pd 
import numpy as np 
import datetime as dt
from sqlalchemy import create_engine
import pymysql
import os
import webbrowser
from fake_useragent import UserAgent
from retrying import retry
import re


# res = pd.DataFrame()
ua = UserAgent(use_cache_server=True)

keyword = "울산광역시 남구"
gubun = 4 

url = "https://m.land.naver.com/search/result/" + keyword

headers = {'User-Agent': ua.random,}
    
res =requests.get(url, headers=headers)    
time.sleep(3)
res.raise_for_status()
soup = BeautifulSoup(res.text, "lxml")


print("res.text=", type(res.text), res.text) 
print("soup", type(soup), soup)


# filter내 정보를 가져오기 위해 split을 이용한 수작업 진행
# re.함수를 이용하여 list로 저장
filter1 = res.text.split('filter: {', 1)
filter2 = filter1[1].split('},', 1)
filter3 = filter2[0].lstrip().rstrip() # 최종 "lat:--, lon:--, ..." 정보만 얻어옴


# re.escape 함수는 문자열을 입력받으면 특수문자들을 이스케이프 처리시켜 준다.
# re.compile은 컴파일을 미리 해 두고 이를 저장할 수 있다
regex = re.compile('{}(.*){}'.format(re.escape("'"), re.escape("'")))
filter_lists = regex.findall(filter3)
# print(filter_lists) # ['37.4937', '126.8823', '14', '1153010200', '구로동', '*', 'A1:B1:B2']

lat = filter_lists[0]
lon = filter_lists[1]
z = filter_lists[2]
cortarNo = filter_lists[3]

print(lat,lon,z,cortarNo)