WITH pos AS
(
	SELECT  bot
	       ,instId
	       ,MAX(updatedAt)                                                                                                   AS latest
	       ,last
	       ,availPos
	       ,avgPx
	       ,pos
	       ,posSide
	       ,upl
	       ,mgnRatio
	       ,notionalUsd
	       ,substring_index(instId,'-',1)                                                                                    AS coin
	       ,CASE WHEN instId = MIN(instId) OVER (partition by bot,substring_index(instId,'-',1)) THEN 'NEAR'  ELSE 'FAR' END AS class
	FROM falcon.positions_h
	WHERE updatedAt > DATE_SUB(NOW(), INTERVAL '5' MINUTE)
	GROUP BY  bot
	         ,instId
), ind AS
(
	SELECT  bot
	       ,name
	       ,alias
	FROM falcon.indexes
	GROUP BY  bot
	         ,name
	         ,alias
), near AS
(
	SELECT  *
	FROM pos
	WHERE class='NEAR' 
), far AS
(
	SELECT  *
	FROM pos
	WHERE class='FAR' 
) , pair AS
(
	SELECT  near.latest
	       ,near.bot
	       ,near.coin
	       ,near.instId                                                       AS nearInst
	       ,far.instId                                                        AS farInst
	       ,near.last                                                         AS near_p
	       ,far.last                                                          AS far_p
	       ,near.pos                                                          AS near_q
	       ,far.pos                                                           AS far_q
	       ,near.upl+far.upl                                                  AS upl
	       ,near.mgnRatio
	       ,near.notionalUsd + far.notionalUsd                                AS notional
	       ,CASE WHEN near.posSide = 'long' THEN 'TOOHIGH'  ELSE 'TOOLOW' END AS dir
	FROM near
	JOIN far
	ON near.bot=far.bot AND near.coin=far.coin
), val AS
(
	SELECT  *
	FROM valuation
	WHERE updatedAt > date_sub(now(), interval '5' MINUTE) 
)
SELECT  ind.name
       ,ind.alias
       ,pair.*
       ,v.totalBal
       ,v.funding
       ,v.trading
       ,v.updatedAt
FROM ind
JOIN pair
ON ind.bot=pair.bot
JOIN val v
ON pair.bot=v.bot AND pair.latest=v.updatedAt
ORDER BY alias, bot;