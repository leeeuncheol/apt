 use crawl_data;
 CREATE TABLE apt2_Rent (
  idx        INT NOT NULL AUTO_INCREMENT,
  갱신요구권사용     VARCHAR(20),
  건축년도   int,
  계약구분   VARCHAR(20),
  계약기간   VARCHAR(50),  
  년    int,  
  법정동  VARCHAR(20),
  보증금액  int,
  아파트 VARCHAR(100),
  월    int,
  월세금액 int,
  일     int,
  전용면적    decimal,
  종전계약보증금    VARCHAR(50),
  종전계약월세    VARCHAR(50),
  지번    VARCHAR(20),
  지역코드    VARCHAR(20),
  층    int,
  시    VARCHAR(20),
  구    VARCHAR(20),
  거래금액범위    VARCHAR(20),
  등록일자    VARCHAR(20),
   PRIMARY KEY(idx)
 ) ;

-- select * from apt2;


update crawl_data.apt2_rent 
set 거래금액범위 = 
	 case 
		when (거래금액 between 0 and 9999) then '1억 미만'
        when (거래금액 between 10000 and 19999) then '1억 이상 2억 미만'
        when (거래금액 between 20000 and 29999) then '2억 이상 3억 미만'
        when (거래금액 between 30000 and 39999) then '3억 이상 4억 미만'
        when (거래금액 between 40000 and 49999) then '4억 이상 5억 미만'
        when (거래금액 between 50000 and 59999) then '5억 이상 6억 미만'
        when (거래금액 between 60000 and 69999) then '6억 이상 7억 미만'
        when (거래금액 between 70000 and 79999) then '7억 이상 8억 미만'
        when (거래금액 between 80000 and 89999) then '8억 이상 9억 미만'
        when (거래금액 between 90000 and 99999) then '9억 이상 10억 미만'
        when (거래금액 between 100000 and 109999) then '10억 이상 11억 미만'
        when (거래금액 between 110000 and 119999) then '11억 이상 12억 미만'
        when (거래금액 between 120000 and 129999) then '12억 이상 13억 미만'
        when (거래금액 between 130000 and 139999) then '13억 이상 14억 미만'
        when (거래금액 between 140000 and 149999) then '14억 이상 15억 미만'
        when (거래금액 between 150000 and 159999) then '15억 이상 16억 미만'
        else '16억 이상'
	END
where 거래금액범위 is null ;   


update crawl_data.apt2_rent 
	set 시 = substring_index(지역코드, ' ', 1)
    where 시 is null ; 
    
update crawl_data.apt2_rent
	set 구 = substring_index(지역코드, ' ', -1)
    where 구 is null ; 


select 월, 일, 시, 구, 법정동, 아파트, 전용면적, 보증금액, 월세금액 , 등록일자 FROM crawl_data.apt2_rent;



select * FROM crawl_data.apt2_rent;

