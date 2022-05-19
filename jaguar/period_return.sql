WITH curr AS
(
	SELECT  bot
	       ,marginBalance
	       ,DATE_SUB(timestamp,INTERVAL EXTRACT(SECOND
	FROM timestamp) SECOND) AS timestamp
	FROM jaguar.account_info
	WHERE asset = "USDT"
	AND HOUR(timestamp) = 0
	AND MINUTE(timestamp) = 0
	ORDER BY timestamp ASC 
), prev_1d AS
(
	SELECT  bot
	       ,timestamp
	       ,LAG(marginBalance,1) OVER (PARTITION BY bot ORDER BY timestamp) AS prev_1d_bal
	FROM curr
	WHERE marginBalance > 0
	ORDER BY timestamp DESC 
), prev_1w AS
(
	SELECT  bot
	       ,timestamp
	       ,LAG(marginBalance,7) OVER (PARTITION BY bot ORDER BY timestamp) AS prev_1w_bal
	FROM curr
	WHERE marginBalance > 0
	ORDER BY timestamp DESC 
)
SELECT  bot_info.name
       ,curr.bot
       ,curr.timestamp
       ,curr.marginBalance                            AS curr_bal
       ,d.prev_1d_bal
       ,w.prev_1w_bal
       ,o.marginBalance                               AS initial
       ,ROUND(curr.marginBalance/d.prev_1d_bal-1,4)   AS daily_ret
       ,ROUND(curr.marginBalance/w.prev_1w_bal-1,4)   AS weekly_ret
       ,ROUND(curr.marginBalance/o.marginBalance-1,4) AS cumulative_ret
FROM curr
JOIN
(
	SELECT  *
	FROM prev_1w
	GROUP BY  bot
) w
ON w.bot = curr.bot AND w.timestamp = curr.timestamp
JOIN
(
	SELECT  *
	FROM prev_1d
	GROUP BY  bot
) d
ON d.bot = curr.bot AND d.timestamp = curr.timestamp
JOIN
(
	SELECT  *
	FROM curr
	WHERE timestamp > "2022-04-27" -- 초기 자본금이 납입된 최초의 시기로 조정함.
	GROUP BY  bot
) o
ON o.bot = curr.bot
JOIN bot_info
ON bot_info.bot = curr.bot
ORDER BY bot;

