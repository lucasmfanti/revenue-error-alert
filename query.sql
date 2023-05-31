SELECT 
    CONCAT('Event: ', CAST(rv.event_id AS STRING)) AS event_id,
    CONCAT('Value: ', CAST(rv.value AS STRING)) AS value,
    CONCAT('Customer: ', RPAD(cu.name, 20)) AS customer
FROM `database.revenues` rv
LEFT JOIN `database.events` ev
    ON rv.event_id = ev.id
INNER JOIN `database.customers` cu
    ON ev.customer_id = cu.id
WHERE TRUE
    AND value_class = 'Revenue'
    AND value < 1
    AND DATETIME(rv.created_at) > DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 4 HOUR)
ORDER BY event_id ASC
