SELECT * FROM crawl_data.apt where 구 = '중구';
 -- select count(*) from crawl_data.apt;

--  ALTER TABLE crawl_data.apt modify 거래금액 int;
--  ALTER TABLE crawl_data.apt modify 전용면적 decimal;
-- ALTER TABLE crawl_data.apt modify 년 int;
-- ALTER TABLE crawl_data.apt modify 월 int;
-- ALTER TABLE crawl_data.apt modify 일 int;
-- ALTER TABLE crawl_data.apt modify 건축년도 int;

 -- truncate crawl_data.apt;  
 
--  alter table crawl_data.apt add 거래금액범위 varchar(100);
--  alter table crawl_data.apt add 시 varchar(100);
--  alter table crawl_data.apt add 구 varchar(100);
--  
-- ALTER TABLE crawl_data.apt DROP 도;


update crawl_data.apt 
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
        else 'N/A'
	END
where 거래금액범위 is null ;   


-- update crawl_data.apt 
-- set 지역코드 = 
-- 	 case 
-- 		when (지역코드 = 31110) then '울산광역시 중구'
--         when (지역코드 = 31140) then '울산광역시 남구'
--         when (지역코드 = 31170) then '울산광역시 동구'
--         when (지역코드 = 31200) then '울산광역시 북구'
--         when (지역코드 = 31710) then '울산광역시 울주군'
--         else 'N/A'
-- 	END
     


update crawl_data.apt 
	set 시 = substring_index(지역코드, ' ', 1);
    
update crawl_data.apt 
	set 구 = substring_index(지역코드, ' ', -1);
