WITH bef AS
(
	SELECT  bot
	       ,updatedAt
	       ,totalBal
	FROM falcon.valuation
	WHERE updatedAt = '2022-02-18 18:00:00'
) , aft AS
(
	SELECT  bot
	       ,updatedAt
	       ,totalBal
	FROM falcon.valuation
	WHERE updatedAt = '2022-02-21 11:00:00'
) , ind AS
(
	SELECT  bot
	       ,name
	       ,alias
	       ,limit_type
	FROM falcon.indexes
	GROUP BY  bot
	         ,name
	         ,alias
	         ,limit_type
)
SELECT  ind.name
       ,ind.alias
       ,ind.limit_type
       ,bef.bot
       ,bef.updatedAt                                            AS beforeDate
       ,aft.updatedAt                                            AS afterDate
       ,bef.totalBal                                             AS beforeValue
       ,aft.totalBal                                             AS afterValue
       ,concat(round((aft.totalBal/bef.totalBal-1) * 100,4),'%') AS ret
FROM bef
JOIN aft
JOIN ind
ON bef.bot=aft.bot AND aft.bot=ind.bot
ORDER BY ind.alias, ind.limit_type, ind.bot ;