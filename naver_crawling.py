from ast import keyword
from email import header
import requests
from bs4 import BeautifulSoup
import json
import re
import math
import sys
import time, datetime
import csv 

import numpy as np
import pandas as pd 
import seaborn as sns
import timeit

keyword = "울산광역시 북구 명촌동" 

#매물 구분 : 1=매매, 2=전세, 3=월세, 4=ALL
gubun = 4

url = "https://m.land.naver.com/search/result/" + keyword
headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'}

res = requests.get(url, headers = headers)
time.sleep(3)
res.raise_for_status()
soup = BeautifulSoup(res.text, "lxml")

print("res.text = ", type(res.text), res.text)

print("Soup", type(soup))


