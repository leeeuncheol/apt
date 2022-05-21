import pymysql
import pandas as pd

db = pymysql.connect( user='crawl_user', passwd='test001', host='localhost', db='crawl_data', charset='utf8' )

cursor = db.cursor(pymysql.cursors.DictCursor)

select_sql = "select * from apt" 

cursor.execute(select_sql) 

result = cursor.fetchall() 

df = pd.DataFrame(result)

df['index'] = df.index + len(df.index)

print(df)


