SELECT %s
FROM %s
WHERE
    TENANT = ?
  AND
    time_slot = ?
  AND
    SERIES_SET_HASH = ?
  AND
    AGGREGATOR = ?
  AND
    ts >= ?
  AND
    ts < ?
