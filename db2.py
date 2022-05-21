from email import header
from sqlite3 import Cursor
import MySQLdb
import requests
from bs4 import BeautifulSoup

if __name__ == "__main__":
    RANK = 100

    header = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Trident/7.0; rv:11.0) like Gecko'}
         
    # 주간 차트를 크롤링 할 것임
    req = requests.get('https://www.melon.com/chart/week/index.htm', headers=header) 

    html = req.text
    parse = BeautifulSoup(html, 'html.parser')


    titles = parse.find_all("div", {"class": "ellipsis rank01"}) 
    singers = parse.find_all("div", {"class": "ellipsis rank02"}) 
    title = [] 
    singer = []

    for t in titles: 
        title.append(t.find('a').text)

    for s in singers: 
        singer.append(s.find('span', {"class": "checkEllipsis"}).text) 
    items = [item for item in zip(title, singer)]



#DB연결
conn = MySQLdb.connect(
    user="crawl_user",
    passwd = "test001",
    host = "localhost",
    db="crawl_data"
)

cursor = conn.cursor()

cursor.execute("Drop table if exists melon")

cursor.execute("CREATE TABLE melon (`rank` int, title text, url text)")

i = 1 
for item in items:
    cursor.execute(
        f"INSERT INTO melon VALUES({i},\"{item[0]}\",\"{item[1]}\")")
    i += 1

conn.commit()
conn.close()    



