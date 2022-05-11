SELECT  bot
       ,totalBal
       ,updatedAt
       ,100 * (totalBal / (CASE WHEN bot IN ('1','3' ,'5','7','8') THEN 500 ELSE 3000 END) - 1) AS ret
FROM
(
	SELECT  bot
	       ,totalBal
	       ,updatedAt
	       ,ROW_NUMBER() OVER (PARTITION BY bot ORDER BY updatedAt DESC) AS rn
	FROM valuation
	WHERE bot not IN ('A', 'B', 'C', 'D')
	AND updatedAt > DATE_SUB(now(), interval '5' minute)
) v
WHERE rn = 1
ORDER BY bot





