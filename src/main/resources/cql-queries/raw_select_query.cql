SELECT %s
FROM %s
WHERE
    tenant = ?
AND
    time_slot = ?
AND
    series_set_hash = ?
AND
    ts >= ?
AND
    ts < ?