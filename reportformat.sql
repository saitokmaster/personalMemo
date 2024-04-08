-- Postgres/Redshift
-- This is sample query for regular metrics report such as weekly/monthly/quarterly.
-- 
-- [sample table: user_visit]
-- visit_user_id VARCHAR
-- user_visit_date DATE
-- user_visit_timestamp TIMESTAMP
-- user_action VARCHAR  -- READ/CLICK

-- [sample table: user_profile]
-- userid VARCHAR
-- region VARCHAR
-- country VARCHAR
-- city VARCHAR
-- sex VARCHAR
-- birthday DATE


-- sample1 extract total user visit count + avarage in weekly, monthly + year to date(YTD)
SELECT
       'WEEKLY' as report_period
      ,DATE_TRUNC(week,user_visit_timestamp) AS report_date
      ,COUNT(user_visit_timestamp) AS user_total_visit_count
      ,AVG(user_total_visit_count) AS average_visit_count
  FROM user_visit a
 WHERE user_visit_date >= DATE_TRUNC(year,current_date)
   AND user_action = 'READ'
GROUP BY 1,2
UNION ALL
SELECT
       'MONTHLY' as report_period
      ,DATE_TRUNC(month,user_visit_timestamp) AS report_date
      ,COUNT(user_visit_timestamp) AS user_total_visit_count
      ,AVG(user_total_visit_count) AS average_visit_count
  FROM user_visit a
 WHERE user_visit_date >= DATE_TRUNC(year,current_date)
   AND user_action = 'READ'
GROUP BY 1,2
UNION ALL
SELECT
       'YTD' as report_period
      ,DATE_TRUNC(year,user_visit_timestamp) AS report_date
      ,COUNT(user_visit_timestamp) AS user_total_visit_count
      ,AVG(user_total_visit_count) AS average_visit_count
  FROM user_visit a
 WHERE user_visit_date >= DATE_TRUNC(year,current_date)
   AND user_action = 'READ'
GROUP BY 1,2
;

-- sample2 collect user count, click rate based on visit count by region + all in YTD
WITH cte_reg AS(
SELECT
       'YTD' as report_period
       ,'ALL' as region
       ,DATE_TRUNC(year,user_visit_timestamp) AS report_date
       ,COUNT(DISTINCT visit_user_id) AS user_count
       ,COUNT(CASE WHEN user_action = 'READ' THEN user_visit_timestamp ELSE NULL END) AS user_total_visit_count
       ,COUNT(CASE WHEN user_action = 'CLICK' THEN user_visit_timestamp ELSE NULL END) AS user_total_click_count
       ,ROUND(user_total_click_count/user_total_visit_count,2) AS click_rate
 FROM user_visit
WHERE user_visit_date >= DATE_TRUNC(year,current_date)
GROUP BY 1,2,3
 ) cte_all AS (
SELECT
       'YTD' as report_period
       ,b.region as region
       ,DATE_TRUNC(year,a.user_visit_timestamp) AS report_date
       ,COUNT(DISTINCT a.visit_user_id) AS user_count
       ,COUNT(CASE WHEN a.user_action = 'READ' THEN a.user_visit_timestamp ELSE NULL END) AS user_total_visit_count
       ,COUNT(CASE WHEN a.user_action = 'CLICK' THEN a.user_visit_timestamp ELSE NULL END) AS user_total_click_count
       ,ROUND(a.user_total_click_count/a.user_total_visit_count,2) AS click_rate
 FROM user_visit a
 INNER JOIN user_profile b ON a.visit_user_id = b.user_id
WHERE a.user_visit_date >= DATE_TRUNC(year,current_date)
GROUP BY 1,2,3
 )

SELECT
	report_period
       ,region
       ,report_date
       ,click_rate
  FROM cte_reg
UNION ALL
SELECT
	report_period
       ,region
       ,report_date
       ,click_rate
  FROM cte_all

-- sample3 collec top10 countries in visit rate + visit user count
WITH cte AS(
SELECT
        b.country
       ,COUNT(DISTINCT a.visit_user_id) AS user_count
       ,COUNT(CASE WHEN a.user_action = 'READ' THEN a.user_visit_timestamp ELSE NULL END) AS user_total_visit_count
       ,COUNT(CASE WHEN a.user_action = 'CLICK' THEN a.user_visit_timestamp ELSE NULL END) AS user_total_click_count
       ,ROUND(a.user_total_click_count/a.user_total_visit_count,2) AS click_rate
       ,RANK() OVER(PARTITION BY b.country, ORDER BY click_rate DESC) AS r
 FROM user_visit a
 INNER JOIN user_profile b ON a.visit_user_id = b.user_id
WHERE a.user_visit_date >= DATE_TRUNC(year,current_date)
GROUP BY 1
 )

SELECT 
	country
       ,r as ranking
       ,click_rate
       ,user_count
  FROM 
	cte
 WHERE r <= 10;
