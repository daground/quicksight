WITH tmp AS (
  SELECT raw.*, 
  ROW_NUMBER() OVER (PARTITION BY bot ORDER BY timestamp ASC) AS rn_asc,
  ROW_NUMBER() OVER (PARTITION BY bot ORDER BY timestamp DESC) AS rn_desc,
  avg(balance) OVER (partition by bot) as mean,
  lag(balance,1) over (partition by bot order by timestamp asc) as lagg 
  FROM falcon.quant_total_vaule_1d AS raw
) 
  
SELECT 
		c.bot, d.first_date, d.last_date, 
		d.first_bal, d.last_bal, 
        round(d.cum_ret,4) as cum_ret, 
        round(d.apy,4) as APY, 
        round(c.std,4) as std, 
        round(d.apy/c.std,4) as sharpe_ratio,
        round(loss.mdd,4) as mdd
FROM
	(SELECT 
		bot, std(balance/lagg-1) * sqrt(365) as std
	FROM
		tmp
	WHERE
		rn_asc > 1
	GROUP BY bot) c
	JOIN
	(SELECT asc_.bot, 
			asc_.timestamp as first_date, desc_.timestamp as last_date,
			asc_.balance as first_bal, desc_.balance as last_bal,
			desc_.balance/asc_.balance - 1 as cum_ret,
			power(desc_.balance/asc_.balance, 365/46) - 1 as apy
	FROM
		(SELECT * FROM tmp WHERE rn_asc = 1) asc_
		JOIN
		(SELECT * FROM tmp WHERE rn_desc = 1) desc_
		ON asc_.bot=desc_.bot) d
	ON c.bot=d.bot 
	JOIN
    (SELECT 
		bot, timestamp, balance, min(balance/cummax-1) as mdd
	FROM 
		(SELECT 
			bot, timestamp, balance, 
			MAX(balance) over (partition by bot order by mean ASC, timestamp ASC) as cummax 
		FROM 
			tmp) dd 
	GROUP BY
		bot) loss
	ON loss.bot=c.bot ;