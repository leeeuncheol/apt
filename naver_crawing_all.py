import json
import time
from multiprocessing.connection import wait
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


res = pd.DataFrame()
ua = UserAgent(use_cache_server=True)


# 네이버 한페이지에 20개 밖에 로드 안시킴. 넉넉히 300페이지 까지 탐색하고 결과값 없으면 break
for j in range(1, 300):

    # 지역명으로 lat, long ,z 정보 뽑아서 url 구성 할 것
    
    url = f"https://m.land.naver.com/cluster/ajax/articleList?rletTpCd=APT&tradTpCd=A1%3AB1%3AB2&z=12&lat=35.5151389&lon=129.2650553&btm=35.3908266&lft=129.048247&top=35.639259&rgt=129.4818636&showR0=&totCnt=5032&cortarNo=3114000000&page={j}"

    # headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.151 Whale/3.14.134.62 Safari/537.36'}
    headers = {'User-Agent': ua.random,}
    
    response = requests.request("GET", url, headers=headers)
    time.sleep(3)

    # print(response.text)

    try : 
        data = json.loads(response.text)
    except : 
        url = "https://m.land.naver.com/index"
        webbrowser.open(url)
        break
        
    # print(data)
    items = data['body']
    # print(items[0])

    df = pd.DataFrame.from_dict(items)
    # print(df[['spc2','hanPrc']])

    # 호출되는 모든 apt별 page concat 
    if  j == 1:
        res = df
    else :
       res = pd.concat([res,df], ignore_index=True)

    # print(f'{i} - {j} - {len(df)}')
    
    # page호출 결과 없으면 for문 중단
    if len(df) == 0 : 
        break
    
    



#필요한 Column만 골라 씀
# sortRes = res[['atclNm','bildNm', 'flrInfo', 'spc2','tradTpNm', 'hanPrc']].sort_values(by=['hanPrc'],ascending=False)
# print(res.info())
res = pd.DataFrame(res[['atclNm','bildNm', 'flrInfo', 'spc2','tradTpNm', 'prc', 'rentPrc', 'hanPrc','direction', 'atclNo','atclFetrDesc','atclCfmYmd']])
res['date'] = dt.datetime.now().date()
res['date'] = pd.to_datetime(res['date'])
#POWER BI에서 처리
# finalRes['atclNo'] = "https://m.land.naver.com/article/info/" + finalRes['atclNo']

#호가 string type 그대로 따로 저장
# res['호가'] = res['hanPrc']

# 면적, 가격은 Numeric으로 변환
# res['spc2'] = pd.to_numeric(res['spc2'])
# finalRes['hanPrc'] = pd.to_numeric(finalRes['hanPrc'].str.replace('억','').str.replace(',','').str.replace(' ',''))

"""
# 가격 string to int
def priceRe(x) :

    total = 0 
     
    if '억' in x : 

        a = x.split('억')[0]
        b = x.split('억')[1]
        if b == '' : 
            total = int(a) * 10000 
        else :
            
            total = int(a) * 10000 + int(b.replace('/',''))
    else:
        # 월세는 '/'기호 우선 999로 대체
        if '/' in x : 
            total = int(x.replace('/','999'))      
        else :    
            total = int(x)

    return total

res['hanPrc'] = res['hanPrc'].str.replace(',','').apply(lambda x : priceRe(x))
"""


print(res)
# print(res.info())
# print(finalRes.info())

#CSV파일 저장
# finalRes.to_csv(os.path.join("NaverList.csv"), index=False,encoding="euc-kr") 


#DB 저장 (MySQL Connector using pymysql)

pymysql.install_as_MySQLdb()

engine = create_engine('mysql+mysqlconnector://crawl_user:test001@localhost:3306/crawl_data', encoding='utf-8')
conn = engine.connect()

res.to_sql(name='naver_apt_crawling', con=engine, if_exists='append', index = False)

conn.close()
