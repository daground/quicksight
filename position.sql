-- funding account까지 포함되었다면 좋았을 것
SELECT 
    MAX(b.timestamp) AS latestUpdate,
    a.bot,
    a.pair,
    a.long_size,
    a.short_size,
    b.equity,
    b.balance,
    IF(a.position = 1,
        'TOO_LOW',
        'TOO_HIGH') AS direction,
	c.balance as total_balance
FROM
    (SELECT 
        SUBSTRING_INDEX(long_id, '-', 2) AS pair, bot, long_size, short_size, position
    FROM
        falcon.position_info
    WHERE
        long_size != 0) a
        JOIN
    (SELECT 
        timestamp, pair, bot, equity, balance
    FROM
        falcon.account_history
    WHERE
        timestamp > DATE_SUB(NOW(), INTERVAL 10 MINUTE)) b ON a.pair = b.pair AND a.bot = b.bot
	JOIN (SELECT timestamp, bot, balance from falcon.total_value_history where timestamp > DATE_SUB(NOW(), INTERVAL 10 MINUTE)) c
    ON c.timestamp=b.timestamp and c.bot=b.bot
GROUP BY a.pair , a.bot;



