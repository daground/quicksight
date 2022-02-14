-- TODO
-- cancel, partially filled에 PNL이 있는지 확인 WITH long_order AS
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
), ordinary AS
(
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
	         ,L.cycle_id
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
	       ,CASE WHEN (ord.direction,ord.sector) IN (('TOOLOW','ENTRY'),('TOOHIGH','EXIT')) THEN (ord.longOrdPx-ord.shortOrdPx)/(ord.longOrdPx+ord.shortOrdPx)
	             WHEN (ord.direction,ord.sector) IN (('TOOLOW','EXIT'),('TOOHIGH','ENTRY')) THEN (ord.shortOrdPx-ord.longOrdPx)/(ord.shortOrdPx+ord.longOrdPx)  ELSE NULL END AS spread
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
ON co.bot=ind.bot AND co.coin=ind.coin