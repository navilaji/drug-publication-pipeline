-- réaliser une requête SQL simple permettant de trouver le chiffre d’affaires (le montant total des ventes), jour par jour, du 1er janvier 2019 au 31 décembre 2019. Le résultat sera trié sur la date à laquelle la commande a été passée.
-- I've created this query in BigQuery, and date is a key word so we cannont use it as a table column. So I've tr_date (transaction date)
-- instead of date for column name
SELECT format_date("%d/%m/%Y",parse_date("%d/%m/%y",tr_date )) AS date, SUM(prod_price * prod_qty ) AS ventes
FROM  client_tr
WHERE parse_date("%d/%m/%y",tr_date ) BETWEEN "2019-01-01" AND "2019-12-31"
GROUP BY tr_date 
ORDER BY parse_date("%d/%m/%y",tr_date ) ASC;

-- To test the query with a test dataset
WITH client_tr AS
 (SELECT '01/01/19' AS tr_date, 1234 AS order_id, 999 AS client_id,101 AS prop_id, 50 AS prod_price, 1 AS prod_qty UNION ALL
  SELECT '01/01/19' AS tr_date, 1234 AS order_id, 999 AS client_id,102 AS prop_id, 10 AS prod_price, 2 AS prod_qty UNION ALL
  SELECT '01/01/19' AS tr_date, 3456 AS order_id, 845 AS client_id,103 AS prop_id, 5 AS prod_price, 4 AS prod_qty UNION ALL
  SELECT '01/04/19' AS tr_date, 3456 AS order_id, 845 AS client_id,104 AS prop_id, 15 AS prod_price, 3 AS prod_qty UNION ALL
  SELECT '01/04/19' AS tr_date, 3456 AS order_id, 845 AS client_id,105 AS prop_id, 20 AS prod_price, 1 AS prod_qty UNION ALL
  SELECT '01/01/19' AS tr_date, 789 AS order_id, 100 AS client_id,101 AS prop_id, 30 AS prod_price, 2 AS prod_qty UNION ALL
  SELECT '09/01/19' AS tr_date, 789 AS order_id, 100 AS client_id,102 AS prop_id, 40 AS prod_price, 2 AS prod_qty UNION ALL
    SELECT '09/01/19' AS tr_date, 789 AS order_id, 100 AS client_id,101 AS prop_id, 40 AS prod_price, 3 AS prod_qty UNION ALL
  SELECT '09/01/21' AS tr_date, 910 AS order_id, 2 AS client_id,103 AS prop_id, 1 AS prod_price, 2 AS prod_qty UNION ALL
  SELECT '01/01/20' AS tr_date, 910 AS order_id, 2 AS client_id,103 AS prop_id, 1 AS prod_price, 2 AS prod_qty UNION ALL
  SELECT '31/12/19' AS tr_date, 910 AS order_id, 2 AS client_id,103 AS prop_id, 1 AS prod_price, 2 AS prod_qty)
SELECT format_date("%d/%m/%Y",parse_date("%d/%m/%y",tr_date )) AS date, SUM(prod_price * prod_qty ) AS ventes
FROM  client_tr
WHERE parse_date("%d/%m/%y",tr_date ) BETWEEN "2019-01-01" AND "2019-12-31"
GROUP BY tr_date 
ORDER BY parse_date("%d/%m/%y",tr_date ) ASC
;