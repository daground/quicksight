-- TODO
-- cancel, partially filled에 PNL이 있는지 확인
-- szMatchPnL까지 반영한 PnL 만들기 



WITH long_order AS
(
	SELECT  a.clOrdId
	       ,a.bot
	       ,a.instId
	       ,a.avgPx
	       ,a.accFillSz
	       ,a.ordId
	       ,a.px
	       ,a.sz
	       ,a.cTime                                                                AS timestamp
	       ,substring_index(a.instId,'-',1)                                        AS coin
	       ,substring_index(a.clOrdId,'v',3)                                       AS cycle_id
	       ,SUM(pnl) over (partition by a.bot,substring_index(a.clOrdId,'v',3))    AS sum_pnl
	       ,SUM(fee) over (partition by a.bot,substring_index(a.clOrdId,'v',3))    AS sum_fee
	       ,SUM(margin) over (partition by a.bot,substring_index(a.clOrdId,'v',3)) AS sum_margin
	FROM orders a
	WHERE a.ordType='ioc' 
	AND a.state IN ('filled', 'canceled')
	AND side = 'buy' 
), short_order AS
(
	SELECT  a.clOrdId
	       ,a.bot
	       ,a.instId
	       ,a.avgPx
	       ,a.accFillSz
	       ,a.ordId
	       ,a.px
	       ,a.sz
	       ,a.cTime                                                                AS timestamp
	       ,substring_index(a.instId,'-',1)                                        AS coin
	       ,substring_index(a.clOrdId,'v',3)                                       AS cycle_id
	       ,SUM(pnl) over (partition by a.bot,substring_index(a.clOrdId,'v',3))    AS sum_pnl
	       ,SUM(fee) over (partition by a.bot,substring_index(a.clOrdId,'v',3))    AS sum_fee
	       ,SUM(margin) over (partition by a.bot,substring_index(a.clOrdId,'v',3)) AS sum_margin
	FROM orders a
	WHERE a.ordType='ioc' 
	AND a.state IN ('filled', 'canceled')
	AND side = 'sell' 
), mexit AS
(
	SELECT  a.bot
	       ,substring_index(longInst,'-',1) AS coin
	       ,least(a.startTime,b.startTime)  AS startTime
	       ,greatest(a.endTime,b.endTime)   AS endTime
	       ,a.cycle_id
	       ,if(substring_index(longInst,'-',-1) < substring_index(shortInst,'-',-1), 'TOOLOW','TOOHIGH') AS direction
	       ,'MEXIT'                          AS sector
	       ,longInst
	       ,shortInst
	       ,a.avgPx                         AS longOrdPx
	       ,b.avgPx                         AS shortOrdPx
	       ,a.avgPx                         AS longAvgPx
	       ,b.avgPx                         AS shortAvgPx
	       ,a.sz                            AS longOrdSz
	       ,b.sz                            AS shortOrdSz
	       ,a.sz                            AS longTrdSz
	       ,b.sz                            AS shortTrdSz
	       ,a.pnl+b.pnl                     AS pnl
	       ,a.fee+b.fee                     AS fee
	       ,0                               AS margin
	FROM
	(
		SELECT  bot
		       ,SUBSTRING_INDEX(clOrdId,'v',2) AS cycle_id
		       ,'name'
		       ,MIN(uTime)                     AS startTime
		       ,MAX(uTime)                     AS endTime
		       ,SUM(sz)                        AS sz
		       ,SUM(avgPx * sz)/SUM(sz)        AS avgPx
		       ,SUM(pnl)                       AS pnl
		       ,SUM(fee)                       AS fee
		       ,instId                         AS longInst
		       ,ordType
		FROM orders
		WHERE SUBSTRING_INDEX(clOrdId, 'v', 1)='3'
		AND state='filled'
		AND side='buy'
		GROUP BY  SUBSTRING_INDEX(clOrdId,'v',2)
	) a
	JOIN
	(
		SELECT  bot
		       ,SUBSTRING_INDEX(clOrdId,'v',2) AS cycle_id
		       ,'name'
		       ,MIN(uTime)                     AS startTime
		       ,MAX(uTime)                     AS endTime
		       ,SUM(sz)                        AS sz
		       ,SUM(avgPx * sz)/SUM(sz)        AS avgPx
		       ,SUM(pnl)                       AS pnl
		       ,SUM(fee)                       AS fee
		       ,instId                         AS shortInst
		       ,ordType
		FROM orders
		WHERE SUBSTRING_INDEX(clOrdId, 'v', 1)='3'
		AND state='filled'
		AND side='sell'
		GROUP BY  SUBSTRING_INDEX(clOrdId,'v',2)
	) b
	ON a.bot=b.bot AND a.cycle_id=b.cycle_id
), ordinary AS
( (
	SELECT  L.bot
	       ,L.coin
	       ,least(L.timestamp,S.timestamp)                                                                                                      AS startTime
	       ,greatest(L.timestamp,S.timestamp)                                                                                                   AS endTime
	       ,L.cycle_id                                                                                                                          AS cycleID
	       ,if(substring_index(L.instId,'-',-1) < substring_index(S.instId,'-',-1) XOR substring_index(L.clOrdId,'v',1)='1','TOOLOW','TOOHIGH') AS direction
	       ,if(substring_index(L.clOrdId,'v',1)='1','ENTRY','EXIT')                                                                             AS sector
	       ,L.instId                                                                                                                            AS longInst
	       ,S.instId                                                                                                                            AS shortInst
	       ,L.px                                                                                                                                AS longOrdPx
	       ,S.px                                                                                                                                AS shortOrdPx
	       ,L.avgPx                                                                                                                             AS longAvgPx
	       ,S.avgPx                                                                                                                             AS shortAvgPx
	       ,L.sz                                                                                                                                AS longOrdSz
	       ,S.sz                                                                                                                                AS shortOrdSz
	       ,L.accFillSz                                                                                                                         AS longTrdSz
	       ,S.accFillSz                                                                                                                         AS shortTrdSz
	       ,L.sum_pnl+S.sum_pnl                                                                                                                 AS pnl
	       ,L.sum_fee+S.sum_fee                                                                                                                 AS fee
	       ,L.sum_margin+S.sum_margin                                                                                                           AS margin
	FROM long_order L
	JOIN short_order S
	WHERE L.bot=S.bot
	AND L.cycle_id=S.cycle_id
	GROUP BY  L.bot
	         ,L.cycle_id) UNION (
	SELECT  *
	FROM mexit)
	ORDER BY endTime DESC
), size_match AS
(
	SELECT  a.bot
	       ,a.instId
	       ,a.avgPx
	       ,a.accFillSz
	       ,substring_index(a.instId,'-',1)                                     AS coin
	       ,substring_index(a.clOrdId,'v',3)                                    AS cycle_id
	       ,SUM(pnl) over (partition by a.bot,substring_index(a.clOrdId,'v',3)) AS sum_pnl
	       ,SUM(fee) over (partition by a.bot,substring_index(a.clOrdId,'v',3)) AS sum_fee
	FROM orders a
	WHERE a.ordType='market'
	AND a.state='filled' 
), cycle_orders AS
(
	SELECT  ord.bot
	       ,ord.coin
	       ,ord.startTime
	       ,ord.endTime
	       ,substring_index(ord.cycleID,'v',-1)                                                               AS cycleID
	       ,ord.direction
	       ,ord.sector
	       ,ord.longInst
	       ,ord.shortInst
	       ,ord.longOrdPx
	       ,ord.shortOrdPx
	       ,CASE WHEN (ord.direction,ord.sector) IN (('TOOLOW','ENTRY'),('TOOHIGH','EXIT'), ('TOOHIGH','MEXIT')) THEN (ord.longOrdPx-ord.shortOrdPx)/(ord.longOrdPx+ord.shortOrdPx)
	             WHEN (ord.direction,ord.sector) IN (('TOOLOW','EXIT'),('TOOHIGH','ENTRY'), ('TOOLOW','MEXIT')) THEN (ord.shortOrdPx-ord.longOrdPx)/(ord.shortOrdPx+ord.longOrdPx)  ELSE NULL END AS spread
	       ,ord.longAvgPx
	       ,ord.shortAvgPx
	       ,ord.longOrdSz
	       ,ord.shortOrdSz
	       ,ord.longTrdSz
	       ,ord.shortTrdSz
	       ,ord.pnl
	       ,ord.fee
	       ,ord.margin
	       ,CASE WHEN ISNULL(sm.accFillSz) THEN 'Matched'
	             WHEN abs(ord.longTrdSz-ord.shortTrdSz)=sm.accFillSz THEN 'Adjusted'  ELSE 'Not Adjusted' END AS isMatched
	       ,sm.instId                                                                                         AS szMatchInst
	       ,sm.avgPx                                                                                          AS szMatchPx
	       ,sm.accFillSz                                                                                      AS szMatchSz
	       ,sm.sum_pnl                                                                                        AS szMatchPurePnL
	       ,sm.sum_fee                                                                                        AS szMatchFee
	       ,sm.sum_pnl+sm.sum_fee                                                                             AS szMatchPnL
	FROM ordinary ord
	LEFT JOIN size_match sm
	ON ord.bot=sm.bot AND ord.cycleID=sm.cycle_id AND ord.coin=sm.coin
), ind AS
(
	SELECT  bot
	       ,name
	       ,substring_index(pair,'-',1) AS coin
	       ,mean
	       ,std
	FROM indexes
)
SELECT  co.bot
       ,ind.name
       ,co.startTime
       ,co.endTime
       ,co.cycleID
       ,co.direction
       ,co.sector
       ,co.longInst
       ,co.shortInst
       ,co.margin
       ,co.longOrdPx
       ,co.shortOrdPx
       ,co.spread
       ,abs(co.spread-ind.mean)/ind.std AS zscore -- 이 부분은 임시로 작성. 추후에는 mean, std가 orders에 편입될 것.
       ,ind.mean
       ,ind.std
       ,co.longAvgPx
       ,co.shortAvgPx
       ,co.longOrdSz
       ,co.shortOrdSz
       ,co.longTrdSz
       ,co.shortTrdSz
       ,co.pnl
       ,co.fee
       ,co.isMatched
       ,co.szMatchInst
       ,co.szMatchSz
       ,co.szMatchPurePnL
       ,co.szMatchPnL
FROM ind
RIGHT JOIN cycle_orders co
ON co.bot=ind.bot AND co.coin=ind.coin;

SELECT  ind.bot
       ,ind.name
       ,agg.pnl + agg.szMatchPnL AS pnl
       ,agg.fee
       ,agg.total_pnl
FROM
(
	SELECT  bot
	       ,round(SUM(pnl),4)                                             AS pnl
	       ,round(SUM(IF(ISNULL(szMatchPnL),0,szMatchPnL)),4)             AS szMatchPnL
	       ,round(SUM(fee),4)                                             AS fee
	       ,round(SUM(pnl + IF(ISNULL(szMatchPnL),0,szMatchPnL) + fee),4) AS total_pnl
	FROM falcon.cycle_order_list_tmp2
	WHERE startTime > '{start}'
	AND endTime < '{end}'
	GROUP BY  bot
) agg
RIGHT JOIN
(
	SELECT  bot
	       ,name
	FROM indexes
	WHERE alias='week'
	GROUP BY  bot
) ind
ON ind.bot=agg.bot;