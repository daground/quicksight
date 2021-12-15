SELECT a.bot, b.pastTime as past_data, a.latest, b.balance, (a.latest/b.balance-1)*100 as ret, b.date FROM 
(SELECT 
	bot, max(timestamp), balance as latest
FROM 
	falcon.total_value_history 
where 
    timestamp >= DATE_SUB(DATE_FORMAT(now(), '%Y-%m-%d %H:%i:00'), INTERVAL 10 MINUTE)GROUP BY bot) a
JOIN (
(SELECT 
	bot, min(timestamp) as pastTime, balance , 'D' as date
FROM 
	falcon.total_value_history 
where 
    timestamp >= DATE_SUB(DATE_FORMAT(now(), '%Y-%m-%d %H:%i:00'), INTERVAL 1 DAY)GROUP BY bot)
UNION
(SELECT 
	bot, min(timestamp) as pastTime, balance, 'W' as date
FROM 
	falcon.total_value_history 
where 
    timestamp >= DATE_SUB(DATE_FORMAT(now(), '%Y-%m-%d %H:%i:00'), INTERVAL 7 DAY) GROUP BY bot)
UNION
(SELECT 
	bot, min(timestamp) as pastTime, balance, 'M' as date
FROM 
	falcon.total_value_history 
where
    timestamp >= DATE_SUB(DATE_FORMAT(now(), '%Y-%m-%d %H:%i:00'), INTERVAL 30 DAY) GROUP BY bot)) b
ON a.bot = b.bot
;