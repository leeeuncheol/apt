import MySQLdb

conn = MySQLdb.connect(
    user="crawl_user",
    passwd = "test001",
    host ="localhost",
    db = "crawl_data"
)

# 커서 생성하기 
cursor = conn.cursor()


# 실행할 때마다 다른값이 나오지 않게 테이블을 제거해두기 
cursor.execute("DROP TABLE IF EXISTS books") 

# 테이블 생성하기 
cursor.execute("CREATE TABLE books (title text, url text)") 
# 데이터 저장하기 
bookname = "처음 시작하는 파이썬" 
url_name = "www.wikibook.co.kr" 
cursor.execute(f"INSERT INTO books VALUES(\"{bookname}\",\"{url_name}\")") 

# 커밋하기 
conn.commit() 

# 연결종료하기 
conn.close()


