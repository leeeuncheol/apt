import requests

url = "https://new.land.naver.com/api/complexes/single-markers/2.0?cortarNo=3114010600&zoom=16&priceType=RETAIL&markerId=16248&markerType=COMPLEX&selectedComplexNo&selectedComplexBuildingNo&fakeComplexMarker&realEstateType=APT%3AABYG%3AJGC&tradeType=&tag=%3A%3A%3A%3A%3A%3A%3A%3A&rentPriceMin=0&rentPriceMax=900000000&priceMin=0&priceMax=900000000&areaMin=90&areaMax=150&oldBuildYears&recentlyBuildYears&minHouseHoldCount&maxHouseHoldCount&showArticle=false&sameAddressGroup=true&minMaintenanceCost&maxMaintenanceCost&directions=&leftLon=129.3362874&rightLon=129.3637533&topLat=35.5523558&bottomLat=35.5387028"

payload={}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)