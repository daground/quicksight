WITH id AS
(
	SELECT  CASE WHEN v.num=1 THEN '1'
	             WHEN v.num=2 THEN '2'
	             WHEN v.num=3 THEN '3'
	             WHEN v.num=4 THEN '4'
	             WHEN v.num=5 THEN '5'
	             WHEN v.num=6 THEN '6'
	             WHEN v.num=7 THEN '7'
	             WHEN v.num=8 THEN '8'
	             WHEN v.num=9 THEN 'A'
	             WHEN v.num=10 THEN 'B'
	             WHEN v.num=11 THEN 'C'  ELSE 'D' END                                     AS bot
	       ,CASE WHEN v.num=1 THEN '2022-01-30 01:02:15'
	             WHEN v.num=2 THEN '2022-01-25 17:48:45'
	             WHEN v.num=3 THEN '2022-01-30 01:20:41'
	             WHEN v.num=4 THEN '2022-01-26 15:13:35'
	             WHEN v.num=5 THEN '2022-01-30 01:16:50'
	             WHEN v.num=6 THEN '2022-02-01 16:20:50'
	             WHEN v.num=7 THEN '2022-01-27 17:48:45'
	             WHEN v.num=8 THEN '2022-01-30 01:38:50'
	             WHEN v.num=9 THEN '2022-01-30 22:28:46'
	             WHEN v.num=10 THEN '2022-01-30 01:38:51'
	             WHEN v.num=11 THEN '2022-01-30 22:29:15'  ELSE '2022-01-30 01:39:40' END AS init
	FROM
	(
		SELECT  ROW_NUMBER() OVER (order by ts) AS num
		FROM
		(
			SELECT  ts
			FROM valuation
			LIMIT 12
		) val
		LIMIT 12
	) v
	ORDER BY bot
), val AS
(
	SELECT  bot
	       ,totalBal
	       ,updatedAt
	       ,100 * (totalBal / (CASE WHEN bot IN ('1','3','5','6','7','8') THEN 500 ELSE 3000 END) - 1) AS ret
	       ,CASE WHEN bot IN ('1','3','5','6','7','8') THEN 500  ELSE 3000 END                         AS init_amt
	FROM
	(
		SELECT  bot
		       ,totalBal
		       ,updatedAt
		       ,ROW_NUMBER() OVER (PARTITION BY bot ORDER BY updatedAt DESC) AS rn
		FROM valuation
		WHERE updatedAt > DATE_SUB(now(), interval '1' DAY)
		AND HOUR(updatedAt)=11
		AND MINUTE(updatedAt)=0
		AND SECOND(updatedAt)=0 
	) v
	WHERE rn = 1
	AND bot not IN ('A', 'B', 'C', 'D')
	ORDER BY bot 
), daily_ret AS
(
	SELECT  bot
	       ,updatedAt
	       ,totalBal
	       ,lag(totalBal) OVER (partition by bot ORDER BY updatedAt)            AS lagged
	       ,lag(totalBal) OVER (partition by bot ORDER BY updatedAt)/totalBal-1 AS ret
	FROM valuation
	WHERE HOUR(updatedAt)=11
	AND MINUTE(updatedAt)=0
	AND SECOND(updatedAt)=0 
), sh AS
(
	SELECT  bot
	       ,std(ret)                                                                       AS std_ret
	       ,avg(ret)                                                                       AS avg_ret
	       ,(unix_timestamp(MAX(updatedAt))-unix_timestamp(MIN(updatedAt))) div (60*60*24) AS days
	FROM daily_ret
	GROUP BY  bot
)
SELECT  val.bot
       ,val.updatedAt                                    AS latest
       ,id.init                                          AS init_time
       ,val.init_amt
       ,val.totalBal
       ,round(val.ret,4)                                 AS cum_ret
       ,avg_ret * 365/sh.days                                AS APY
       ,sh.std_ret * sqrt(365/sh.days)                       AS APS
       ,(avg_ret * 365/sh.days)/(sh.std_ret * sqrt(365/sh.days)) AS sharpe_ratio
FROM val
JOIN id
JOIN sh
ON val.bot=id.bot AND id.bot=sh.bot ;