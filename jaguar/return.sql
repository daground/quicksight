WITH account AS
(
	SELECT  bot
	       ,timestamp
	       ,marginBalance
	FROM jaguar.account_info
	WHERE walletBalance > 0
	AND asset='USDT'
	AND HOUR(timestamp)=0
	AND MINUTE(timestamp)=0 
), init_date AS
(
	SELECT  bot
	       ,MIN(timestamp) AS init_ts
	FROM account
	GROUP BY  bot
), end_date AS
(
	SELECT  bot
	       ,MAX(timestamp) AS end_ts
	FROM account
	GROUP BY  bot
), day_ago AS
(
	SELECT  bot
	       ,DATE_SUB(end_ts,INTERVAL 1 DAY) AS day_ago_ts
	FROM end_date
)
SELECT  i.bot
       ,i.init_ts
       ,e.end_ts
       ,g.day_ago_ts
       ,i.start_val
       ,e.end_val
       ,g.day_ago_val
       ,e.end_val/g.day_ago_val-1 AS daily_return
       ,e.end_val/i.start_val-1   AS total_return
FROM
(
	SELECT  d.*
	       ,a.marginBalance AS start_val
	FROM init_date d
	LEFT JOIN account a
	ON d.init_ts=a.timestamp AND d.bot=a.bot
	WHERE d.bot!="t" 
) i
JOIN
(
	SELECT  d.*
	       ,a.marginBalance AS end_val
	FROM end_date d
	LEFT JOIN account a
	ON d.end_ts=a.timestamp AND d.bot=a.bot
	WHERE d.bot!="t" 
) e
JOIN
(
	SELECT  d.*
	       ,a.marginBalance AS day_ago_val
	FROM day_ago d
	LEFT JOIN account a
	ON d.day_ago_ts=a.timestamp AND d.bot=a.bot
	WHERE d.bot!="t" 
) g
ON i.bot=e.bot AND e.bot=g.bot ;


WITH curr AS
(
	SELECT  bot
	       ,marginBalance
	       ,updateTime
	       ,timestamp
	FROM jaguar.account_info
	WHERE asset = "USDT"
	AND HOUR(timestamp) = 0
	AND MINUTE(timestamp) = 0
	ORDER BY timestamp DESC 
), day_ago AS
(
	SELECT  *
	FROM curr AND timestamp < from_unixtime
	(unix_timestamp() - 60*1440
	)
	LIMIT 1
), week_ago AS
(
	SELECT  *
	FROM curr AND timestamp < from_unixtime
	(unix_timestamp() - 60*1440*7
	)
	LIMIT 1
);

