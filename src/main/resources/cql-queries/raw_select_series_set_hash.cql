SELECT
    series_set_hash
FROM %s
WHERE
    TENANT  = ?
  AND
    TIME_SLOT = ?
