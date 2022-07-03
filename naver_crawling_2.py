import json
import requests
import pandas as pd 
import numpy as np 
import datetime as dt
from sqlalchemy import create_engine
import pymysql
import os
import webbrowser
from fake_useragent import UserAgent
import time
import math
import datetime
res = pd.DataFrame()

#송정 
#명촌돟 평창리비에르2차 : 16248
#화봉동 쌍용예가 : 105026

#문수로아이파크2차 1단지 : 104796
#문수로아이파크2차 2단지 : 104797
#대현 더샵 1 : 111347
#대현 더샵 2 : 111348
#번영로 하늘채 : 132862
#야음 동부 : 6053
#쌍용스윗닷홈 : 12007
#아데라움 : 12053
#옥동대공원한신휴플러스 : 102949

#약사더샵 
#센트리지 : 135601

#서부동 자이1 : 131665
#서부동 자이2 : 131666
#힐스이스턴베이 : 106557
#빅쓰리

#미장아이파크2 : 110132

start = time.time()

#단지 LIST
aptArray = ['16248','105026','132862','6053', '111347', '111348','106557','12053','102949','110132','131665','131666','135601','12007', '104796', '104797']
# aptArray = ['131666','135601','12007', '104796', '104797']


blocked = False
ua = UserAgent()

for i in range(0, len(aptArray)):
    
    if blocked :
        break

    # 네이버 한페이지에 20개 밖에 로드 안시킴. 넉넉히 20페이지 까지 탐색하고 결과값 없으면 break
    for j in range(1, 20):

        url = f"https://m.land.naver.com/complex/getComplexArticleList?hscpNo={aptArray[i]}&cortarNo=3120012000&tradTpCd=A1%3AB1%3AB2&order=point_&showR0=N&page={j}"

        payload={}
        # headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.151 Whale/3.14.134.62 Safari/537.36'}
        headers = {'User-Agent': ua.random,}
        response = requests.request("GET", url, headers=headers, data=payload)
        time.sleep(2)

        # print(response.text)

        try : 
            data = json.loads(response.text)
        except : 
            url = "https://m.land.naver.com/index"
            webbrowser.open(url)
            blocked = True
            break
        
        # print(data)
        items = data['result']['list']
        # print(items[0])

        df = pd.DataFrame.from_dict(items)
        # print(df[['spc2','prcInfo']])

        # 호출되는 모든 apt별 page concat 
        if i == 0 and j == 1:
            res = df
        else :
           res = pd.concat([res,df], ignore_index=True)

        # print(f'{i} - {j} - {len(df)}')
        
        # page호출 결과 없으면 for문 중단
        if len(df) == 0 : 
            break


#필요한 Column만 골라 씀
# sortRes = res[['atclNm','bildNm', 'flrInfo', 'spc2','tradTpNm', 'prcInfo']].sort_values(by=['prcInfo'],ascending=False)
finalRes = pd.DataFrame(res[['atclNm','bildNm', 'flrInfo', 'spc2','tradTpNm', 'prcInfo','atclNo','atclFetrDesc','cfmYmd']])
finalRes['date'] = dt.datetime.now().date()
finalRes['date'] = pd.to_datetime(finalRes['date'])
#POWER BI에서 처리
# finalRes['atclNo'] = "https://m.land.naver.com/article/info/" + finalRes['atclNo']

#호가 string type 그대로 따로 저장
finalRes['호가'] = finalRes['prcInfo']

# 면적, 가격은 Numeric으로 변환
finalRes['spc2'] = pd.to_numeric(finalRes['spc2'])
# finalRes['prcInfo'] = pd.to_numeric(finalRes['prcInfo'].str.replace('억','').str.replace(',','').str.replace(' ',''))

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

finalRes['prcInfo'] = finalRes['prcInfo'].str.replace(',','').apply(lambda x : priceRe(x))

end = time.time()
sec = (end - start)
runtime = str(datetime.timedelta(seconds=sec)).split(".")[0]

print(finalRes)

print(runtime)
# print(finalRes.info())

#CSV파일 저장
# finalRes.to_csv(os.path.join("check.csv"), index=False,encoding="euc-kr") 


#DB 저장 (MySQL Connector using pymysql)

pymysql.install_as_MySQLdb()

engine = create_engine('mysql+mysqlconnector://crawl_user:test001@localhost:3306/crawl_data', encoding='utf-8')
conn = engine.connect()

finalRes.to_sql(name='naver2', con=engine, if_exists='append', index = False)

conn.close()

