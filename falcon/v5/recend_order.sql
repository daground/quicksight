SELECT  name
       ,startTime
       ,direction
       ,sector
       ,SUBSTRING_INDEX(longInst,'-',1) AS coin
       ,longInst
       ,shortInst
       ,zscore
       ,mean
       ,std
       ,spread
       ,longAvgPx
       ,shortAvgPx
       ,margin
       ,pnl + if(isnull(szMatchPnL), 0, szMatchPnL)  + fee as pnl
FROM falcon.cycle_order_list_tmp2
WHERE DATE_SUB(now(), interval 5 DAY);