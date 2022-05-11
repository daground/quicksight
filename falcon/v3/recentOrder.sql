WITH co AS (
	SELECT 
		bot , coin , spread_type , pos_type, end_time, start_time,
		long_cont_price+short_cont_price as unit_pair_cost, size, spread, realized_pnl
	FROM
		falcon.cycle_order_list
	WHERE
		end_time > DATE_SUB(NOW(), INTERVAL 3 DAY))
	, ci AS (
	SELECT 
		bot, SUBSTRING_INDEX(pair,'-',1) as coin, contract_val 
	FROM 
		falcon.pair_info AS ci
	WHERE
		active=1)
	, id AS (
    SELECT 
		bot,
		SUBSTRING_INDEX(pair, '-', 1) AS coin,
		ROUND(mean*10000,2) as mean,
		ROUND((mean + standard)*10000, 2) AS upper,
		ROUND((mean - standard)*10000, 2) AS lower
	FROM
		falcon.input_data
	WHERE
		bot = '2'
	UNION
	SELECT 
		bot,
		SUBSTRING_INDEX(pair, '-', 1) AS coin,
		ROUND(mean * 10000, 2) as mean,
		ROUND((mean + k * standard)*10000, 2) AS upper,
		ROUND((mean - k * standard)*10000, 2) AS lower
	FROM
		falcon.input_data
	WHERE
		bot = '3'
)

SELECT 
	co.bot, co.coin, spread_type, pos_type, 
    min(start_time) as first_order_time,
    max(end_time) as latest_order_time,
    round(sum(unit_pair_cost*size*contract_val),4) as position_size,
    round(sum(unit_pair_cost*size*contract_val/8),4) as used_margin,
    round(sum(realized_pnl),4) as pnl,
    round(avg(spread)* 10000, 2) as avg_spread,
    id.mean, id.upper, id.lower
FROM co 
JOIN 
	ci ON co.bot=ci.bot and co.coin=ci.coin
JOIN
	id ON ci.bot=id.bot and ci.coin=id.coin
GROUP BY
	bot, coin, spread_type, pos_type
ORDER BY
	bot, pos_type, spread_type
;