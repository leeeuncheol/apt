import requests
import datetime
import xml.etree.ElementTree as ET
import pandas as pd

url ="http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade?"
service_key = "HseXRcMtASFE%2FhyAp7e3burfnPh39Mb4xoJuTHBZtzL6YUqYu52JbW4Idd2SLtJDS92y1tIWJ5aUPJ4G7Fcf2Q%3D%3D"
base_date = "202001" 
gu_code = '11215' ## 법정동 코드 5자리라면, 구 단위로 데이터를 확보하는 것. 11215 = 광진구

payload = "LAWD_CD=" + gu_code + "&" + \
          "DEAL_YMD=" + base_date + "&" + \
          "serviceKey=" + service_key + "&" 
          
res = requests.get(url + payload)
print(res)




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
    
items_list = get_items(res)
items = pd.DataFrame(items_list) 
print(items.head())