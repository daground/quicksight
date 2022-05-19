WITH _daily AS
(
	SELECT  bot
	       ,DATE_SUB(timestamp,INTERVAL EXTRACT(SECOND
	FROM timestamp) SECOND) AS timestamp , marginBalance
	FROM jaguar.account_info
	WHERE walletBalance > 0
	AND asset = 'USDT'
	AND MINUTE(timestamp) = 0
	AND HOUR(timestamp) = 0
	AND timestamp < from_unixtime(unix_timestamp() - 9 * 3600 )
	ORDER BY timestamp ASC
), daily AS
(
	SELECT  *
	       ,LAG(marginBalance,1) OVER (PARTITION BY bot ORDER BY timestamp) AS lagging
	FROM _daily
), first_date AS
(
	SELECT  bot
	       ,MIN(timestamp) AS min_ts
	FROM daily
	GROUP BY  bot
), rets AS
(
	SELECT  daily.bot
	       ,timestamp
	       ,marginBalance
	       ,lagging
	       ,(marginBalance-lagging)/lagging AS ret
	FROM daily
	LEFT JOIN first_date
	ON daily.timestamp = first_date.min_ts AND daily.bot = first_date.bot
	WHERE min_ts is NULL
	ORDER BY daily.bot
)
SELECT  bot
       ,round(AVG(ret),4)             AS mean
       ,round(stddev(ret),4)          AS std
       ,round(AVG(ret)/stddev(ret),6) AS sharpe

FROM rets
GROUP BY bot