import pandas as pd
import requests
import MySQLdb
import os
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine
import pymysql

#API 호출에 법정동코드 앞 5자리가 필요
code_file = "법정동코드 전체자료.txt"
code = pd.read_csv(code_file, sep='\t')
## 컬럼명 변경
code.columns = ['code', 'name', 'is_exist']
## 유효한 코드만 사용
code = code [code['is_exist'] == '존재']
## string으로 변경
code['code'] = code['code'].apply(str) 
code['substr'] = code['code'].str[:5]
#지역 필터
filter = code['name'].str.contains("울산광역시") 
subset_df = code[filter]
## 중복제거
subset_df = subset_df.drop_duplicates(['substr'])
## 최종 코드 DateFrame
gu_code_list = subset_df['substr'][1:].reset_index(drop = True)



#추출 하고자하는 기간 설정
year = [str("%02d" %(y)) for y in range(2022, 2023)]
month = [str("%02d" %(m)) for m in range(5, 6)]
base_date_list = ["%s%s" %(y, m) for y in year for m in month ]


##추출 하고자하는 지역 설정
# gu = "울산광역시 울주군"
# gu_code = code[ (code['name'].str.contains(gu) )]
# gu_code = gu_code['code'].reset_index(drop=True)
# gu_code = str(gu_code[0])[0:5]
# print(gu_code)



#API연결
def get_data(gu_code, base_date):
    url ="http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade?"
    service_key = "HseXRcMtASFE%2FhyAp7e3burfnPh39Mb4xoJuTHBZtzL6YUqYu52JbW4Idd2SLtJDS92y1tIWJ5aUPJ4G7Fcf2Q%3D%3D"    
    payload = "LAWD_CD=" + gu_code + "&" + \
              "DEAL_YMD=" + base_date + "&" + \
              "serviceKey=" + service_key + "&"

    res = requests.get(url + payload)
    
    return res



def get_items(response):
    root = ET.fromstring(response.content)
    item_list = []
    for child in root.find('body').find('items'):
        elements = child.findall('*')
        data = {}
        for element in elements:
            tag = element.tag.strip()
            text = element.text.strip()
            # print tag, text
            data[tag] = text
        item_list.append(data)  
    return item_list



#월별로 데이터 요청을 하고 쌓아감.
items_list = []
for base_date in base_date_list:
    for gu_code in gu_code_list : 
        res = get_data(gu_code, base_date)
        items_list += get_items(res)
    
## dataframe 생성
items = pd.DataFrame(items_list) 

## data type 변경
items['거래금액'] = pd.to_numeric(items['거래금액'].str.replace(',',''))
items['전용면적'] = pd.to_numeric(items['전용면적'])
items['년'] = pd.to_numeric(items['년'])
items['월'] = pd.to_numeric(items['월'])
items['일'] = pd.to_numeric(items['일'])
items['층'] = pd.to_numeric(items['층'])
items['건축년도'] = pd.to_numeric(items['건축년도'])


items['지역코드'] = items['지역코드'] + '00000'

gu_code_name = code[code['code'].isin(items['지역코드'])][['code','name']]
gu_name_dict = gu_code_name.set_index('code').T.to_dict('index')['name']

items = items.replace({'지역코드' : gu_name_dict })

#data check
print(items.head())


#CSV파일 저장
# items.to_csv(os.path.join("%s_%s~%s.csv" %(gu, year[0], year[-1])), index=False,encoding="euc-kr") 


#DB 저장 
# MySQL Connector using pymysql

pymysql.install_as_MySQLdb()

engine = create_engine('mysql+mysqlconnector://crawl_user:test001@localhost:3306/crawl_data', encoding='utf-8')
conn = engine.connect()

items.to_sql(name='apt2', con=engine, if_exists='append', index = False)

conn.close()


