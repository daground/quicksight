SELECT  bot
       ,timestamp
       ,symbol
       ,unrealizedProfit
       ,entryPrice
       ,positionSide
       ,positionAmt
       ,notional
FROM jaguar.position
WHERE notional != 0
AND timestamp > from_unixtime(unix_timestamp() -3600*9 - 60)
AND timestamp < from_unixtime(unix_timestamp() -3600*9)
ORDER BY bot ASC, notional ASC ;