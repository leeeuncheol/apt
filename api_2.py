import pandas as pd
import numpy as np
import requests
import MySQLdb
import os
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine
import pymysql
import random
import string

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
month = [str("%02d" %(m)) for m in range(6, 7)]
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
items['등록일자'] = pd.datetime.now().date()


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



"""
def table_column_names(table: str) -> str:
 
    query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'"
    rows = engine.execute(query)
    dirty_names = [i[0] for i in rows]
    clean_names = '`' + '`, `'.join(map(str, dirty_names)) + '`'
    return clean_names



def insert_conflict_ignore(df: pd.DataFrame, table: str, index: bool):
    
    # generate random table name for concurrent writing
    temp_table = ''.join(random.choice(string.ascii_letters) for i in range(10))
    try:
        df.to_sql(temp_table, engine, index=index)
        columns = table_column_names(table=temp_table)
        insert_query = f'INSERT IGNORE INTO {table}({columns}) SELECT {columns} FROM `{temp_table}`'
        engine.execute(insert_query)
    except Exception as e:
        print(e)        

    # drop temp table
    drop_query = f'DROP TABLE IF EXISTS `{temp_table}`'
    engine.execute(drop_query)


def save_dataframe(df: pd.DataFrame, table: str):
   
    if df.index.name is None:
        save_index = False
    else:
        # save_index = True
        save_index = False

    insert_conflict_ignore(df=df, table=table, index=save_index)


save_dataframe(items, 'apt2')
"""

# DB에 중복되면 안되는 열들을 제외하고 새로 추가해야 할 Data들만 업데이트하기
# df: 새로 업데이트 할 Pandas DataFrame
# tablename: SQL의 테이블명
# engine: SQL connector
# dup_cols: 중복 여부를 결정할 key columns
# filter_continuous_col: where문으로 제어할 continuous columns
# filter_categorical_col: where문으로 제어할 categorical columns
def filter_new_df(df, tablename, engine, dup_cols=[],
                         filter_continuous_col=None, filter_categorical_col=None):
    
    args = 'SELECT %s FROM %s' %(', '.join(['{0}'.format(col) for col in dup_cols]), tablename)
    print(args)
    args_contin_filter, args_cat_filter = None, None
    
    if filter_continuous_col is not None:
        if df[filter_continuous_col].dtype == 'datetime64[ns]':
            args_contin_filter = """ "%s" BETWEEN Convert(datetime, '%s')
                                          AND Convert(datetime, '%s')""" %(filter_continuous_col,
                              df[filter_continuous_col].min(), df[filter_continuous_col].max())


    if filter_categorical_col is not None:
        args_cat_filter = ' "%s" in(%s)' %(filter_categorical_col,
                          ', '.join(["'{0}'".format(value) for value in df[filter_categorical_col].unique()]))

    if args_contin_filter and args_cat_filter:
        args += ' Where ' + args_contin_filter + ' AND' + args_cat_filter
    elif args_contin_filter:
        args += ' Where ' + args_contin_filter
    elif args_cat_filter:
        args += ' Where ' + args_cat_filter

    # 기존 최고가 
    oridf = pd.read_sql(args, engine)
    maxoridf = oridf.groupby(['아파트','전용면적'])['거래금액'].agg(**{'최고가':'max'}).reset_index()
    # print(maxoridf)
    
    df.drop_duplicates(dup_cols, keep='last', inplace=True)
    df = pd.merge(df, pd.read_sql(args, engine), how='left', on=dup_cols, indicator=True)
    df = df[df['_merge'] == 'left_only']
    df.drop(['_merge'], axis=1, inplace=True)
    
    df['전용면적'] = np.round(df['전용면적'], decimals = 0)
    df['신고가'] = 'X'
    
    for i in df.index : 
        try : 
            condition1 = maxoridf['아파트'] == df['아파트'][i] 
            condition2 =  maxoridf['전용면적'] == df['전용면적'][i]
            existMax = maxoridf.loc[condition1 & condition2]['최고가'].item()
            print('기존:', existMax, '/ 신규:' , df['거래금액'][i].item())
            
            if df['거래금액'][i].item() > existMax : 
                df['신고가'][i] = 'O'
        except : 
            print('오류발생 :', df['아파트'][i], df['전용면적'][i])
            
    print('*****************') 
    print('신고가율:', len(df[df['신고가'] == 'O'].index) / len(df.index) * 100, '%')
    print('*****************')     
    
    return df


cols = ['거래금액','아파트','층','월','일','전용면적']
newitems = filter_new_df(items, 'apt2', engine, cols, None, None)
print(newitems)

# newitems.to_csv('c:/temp/test.csv', index=False)

newitems.to_sql(name='apt2', con=engine, if_exists='append', index = False)
conn.close()