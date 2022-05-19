SELECT  bot
       ,DATE_SUB(timestamp, INTERVAL EXTRACT(SECOND from timestamp) SECOND) as timestamp
       ,marginBalance
FROM jaguar.account_info
WHERE walletBalance > 0
AND asset='USDT'
AND MINUTE(timestamp)=0 
AND timestamp > from_unixtime(unix_timestamp() - 9 * 3600 - 24 * 3600 * 7 )
AND timestamp < from_unixtime(unix_timestamp() - 9 * 3600 )
ORDER BY timestamp DESC ;