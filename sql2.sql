-- Réaliser une requête un peu plus complexe qui permet de déterminer, par client et sur la période allant du 1er janvier 2019 au 31 décembre 2019, les ventes meuble et déco réalisées.
WITH client_tr_ptype AS (
    SELECT tr.client_id AS client_id, p.product_type AS ptype, SUM(tr.prod_price*tr.prod_qty) AS ventes
    FROM TRANSACTION tr INNER JOIN PRODUCT_NOMENCLATURE p
    ON tr.prop_id = p.product_id
	WHERE parse_date("%d/%m/%y",tr.tr_date ) BETWEEN "2019-01-01" AND "2019-12-31"
    GROUP BY tr.client_id , p.product_type
),
ventes_meuble AS (
    SELECT client_id, ventes
    FROM client_tr_ptype WHERE ptype = 'meuble'
),
ventes_deco AS (
    SELECT client_id, ventes
    FROM client_tr_ptype WHERE ptype = 'deco'
)
SELECT IFNULL(d.client_id, m.client_id) AS client_id,   
        IFNULL(d.ventes,0) AS ventes_deco,
        IFNULL(m.ventes,0) AS ventes_meuble
FROM ventes_deco d  FULL JOIN ventes_meuble m ON d.client_id =m.client_id 
;

-- query with test dataset
WITH TRANSACTION AS
 (SELECT '01/01/19' AS tr_date, 1234 AS order_id, 999 AS client_id,490756 AS prop_id, 50 AS prod_price, 1 AS prod_qty UNION ALL
  SELECT '01/01/19' AS tr_date, 1234 AS order_id, 999 AS client_id,389728 AS prop_id, 10 AS prod_price, 2 AS prod_qty UNION ALL
  SELECT '01/01/19' AS tr_date, 3456 AS order_id, 845 AS client_id,490756 AS prop_id, 5 AS prod_price, 4 AS prod_qty UNION ALL
  SELECT '01/04/19' AS tr_date, 3456 AS order_id, 845 AS client_id,549380 AS prop_id, 15 AS prod_price, 3 AS prod_qty UNION ALL
  SELECT '01/04/19' AS tr_date, 3456 AS order_id, 845 AS client_id,293718 AS prop_id, 20 AS prod_price, 1 AS prod_qty UNION ALL
  SELECT '08/01/19' AS tr_date, 789 AS order_id, 100 AS client_id,490756 AS prop_id, 30 AS prod_price, 2 AS prod_qty UNION ALL
  SELECT '09/01/19' AS tr_date, 789 AS order_id, 100 AS client_id,389728 AS prop_id, 40 AS prod_price, 2 AS prod_qty UNION ALL
  SELECT '09/01/19' AS tr_date, 789 AS order_id, 100 AS client_id,549380 AS prop_id, 40 AS prod_price, 3 AS prod_qty UNION ALL
  SELECT '09/01/19' AS tr_date, 910 AS order_id, 2 AS client_id,549380 AS prop_id, 2 AS prod_price, 1230 AS prod_qty UNION ALL 
  SELECT '09/01/19' AS tr_date, 911 AS order_id, 2 AS client_id,389728 AS prop_id, 5 AS prod_price, 5 AS prod_qty UNION ALL 
  SELECT '09/01/19' AS tr_date, 912 AS order_id, 2 AS client_id,293718 AS prop_id, 10 AS prod_price, 10 AS prod_qty UNION ALL 
  SELECT '09/04/20' AS tr_date, 913 AS order_id, 3 AS client_id,549380 AS prop_id, 10 AS prod_price, 11 AS prod_qty UNION ALL 
  SELECT '09/04/20' AS tr_date, 914 AS order_id, 3 AS client_id,490756 AS prop_id, 10 AS prod_price, 210 AS prod_qty UNION ALL 
  SELECT '11/06/21' AS tr_date, 915 AS order_id, 4 AS client_id,293718 AS prop_id, 100 AS prod_price, 3 AS prod_qty
  
  ),
PRODUCT_NOMENCLATURE AS (
    SELECT 490756 AS product_id, 'meuble' AS product_type, 'chaise' AS product_name UNION ALL
    SELECT 389728 AS product_id, 'deco' AS product_type, 'boule de noel' AS product_name UNION ALL
    SELECT 549380 AS product_id, 'meuble' AS product_type, 'canape' AS product_name UNION ALL
    SELECT 293718 AS product_id, 'deco' AS product_type, 'mug' AS product_name 
),
client_tr_ptype AS (
    SELECT tr.client_id AS client_id, p.product_type AS ptype, SUM(tr.prod_price*tr.prod_qty) AS ventes
    FROM TRANSACTION tr INNER JOIN PRODUCT_NOMENCLATURE p
    ON tr.prop_id = p.product_id
	WHERE parse_date("%d/%m/%y",tr.tr_date ) BETWEEN "2019-01-01" AND "2019-12-31"
    GROUP BY tr.client_id , p.product_type
),
ventes_meuble AS (
    SELECT client_id, ventes
    FROM client_tr_ptype WHERE ptype = 'meuble'
),
ventes_deco AS (
    SELECT client_id, ventes
    FROM client_tr_ptype WHERE ptype = 'deco'
)
SELECT IFNULL(d.client_id, m.client_id) AS client_id,   
        IFNULL(d.ventes,0) AS ventes_deco,
        IFNULL(m.ventes,0) AS ventes_meuble
FROM ventes_deco d  FULL JOIN ventes_meuble m ON d.client_id =m.client_id 
;