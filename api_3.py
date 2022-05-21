import PublicDataReader as pdr


# API KEY 입력
serviceKey="HseXRcMtASFE%2FhyAp7e3burfnPh39Mb4xoJuTHBZtzL6YUqYu52JbW4Idd2SLtJDS92y1tIWJ5aUPJ4G7Fcf2Q%3D%3D"

AptTrade = pdr.AptTradeReader(serviceKey)


# 주소 검색
df_code = AptTrade.CodeFinder("분당구")
df_code.head(1)

# 데이터 수집
df = AptTrade.DataReader("41135", "202004")
df.head()