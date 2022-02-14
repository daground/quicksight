WITH pi AS (
	SELECT 
		SUBSTRING_INDEX(long_id, '-', 2) AS pair, bot, long_size, short_size, position
	FROM
		falcon.position_info
	WHERE
		long_size != 0
	UNION
	SELECT pair, bot, long_size, short_size, position FROM 
		(SELECT distinct(bot) as bot 
        FROM falcon.position_info) bot
		JOIN 
		(SELECT 'funding-USDT' as pair, 1 as long_size, 1 as short_size, 0 as position
		FROM falcon.position_info LIMIT 1) new), 
    ach AS (
    SELECT 
        timestamp, pair, bot, equity, balance
    FROM
        falcon.account_history
    WHERE
        timestamp > DATE_SUB(NOW(), INTERVAL 10 MINUTE)),
    tvh AS (
	SELECT timestamp, bot, balance 
    from falcon.total_value_history 
    where timestamp > DATE_SUB(NOW(), INTERVAL 10 MINUTE)
)

SELECT 
    MAX(ach.timestamp) AS latestUpdate,
    pi.bot,
    pi.pair,
    pi.long_size,
    pi.short_size,
    ach.equity,
    ach.balance,
    CASE
		WHEN pi.position = 1 THEN 'TOO_LOW'
        WHEN pi.position = 2 THEN'TOO_HIGH'
        ELSE 'FUNDING' 
	END AS direction,
	tvh.balance as total_balance
FROM
    pi 
    JOIN ach ON pi.pair = ach.pair AND pi.bot = ach.bot
	JOIN tvh ON tvh.timestamp=ach.timestamp and tvh.bot=ach.bot
GROUP BY pi.pair , pi.bot;



