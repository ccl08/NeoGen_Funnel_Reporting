-- 1. SCALABLE UDFs
CREATE TEMP FUNCTION GetStringParam(params ANY TYPE, param_key STRING) AS (
  (SELECT value.string_value FROM UNNEST(params) WHERE key = param_key LIMIT 1)
);
CREATE TEMP FUNCTION GetIntParam(params ANY TYPE, param_key STRING) AS (
  (SELECT value.int_value FROM UNNEST(params) WHERE key = param_key LIMIT 1)
);

-- 2. Build the KPI Funnel Table (User-level + Session-level)
CREATE OR REPLACE TABLE `neogen-ga4-export.funnelPurchase_table.segment_funnel_kpis_final_v2` AS

WITH raw_events AS (
  SELECT
    PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS date,
    user_pseudo_id,
    event_name,
    event_timestamp,
    CONCAT(user_pseudo_id, CAST(GetIntParam(event_params, 'ga_session_id') AS STRING)) AS session_id,
    GetStringParam(event_params, 'page_location') AS page_location,
    REGEXP_REPLACE(GetStringParam(event_params, 'page_location'), r'(\?|#).*', '') AS join_key,
    IFNULL(REGEXP_EXTRACT(GetStringParam(event_params, 'page_location'), r"https?://[^/]+([^?#]*)"), '/') AS page_path,
    GetStringParam(event_params, 'market_id') AS market_id,
    GetStringParam(event_params, 'priority_region') AS priority_region,
    CAST(GetIntParam(item.item_params, 'item_sku') AS STRING) AS item_sku,
    item.item_category AS product_class,
    item.item_variant AS super_class,
    GetStringParam(item.item_params, 'line_of_business') AS line_of_business,
    GetStringParam(item.item_params, 'item_catalog') AS item_catalog,
    ecommerce.transaction_id
  FROM `neogen-ga4-export.analytics_331328809.events_*`
  LEFT JOIN UNNEST(items) AS item
  WHERE
    _TABLE_SUFFIX BETWEEN '20251201' AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND device.web_info.hostname = 'www.neogen.com'
    AND event_name IN (
      'page_view',
      'screen_view',
      'view_search_results',
      'add_to_cart',
      'view_cart',
      'begin_checkout',
      'purchase',
      'view_item',
      'view_item_list'
    )
),

enriched_events AS (
  SELECT
    *,
    MAX(item_sku)         OVER (PARTITION BY user_pseudo_id, session_id, join_key) AS enriched_sku,
    MAX(product_class)    OVER (PARTITION BY user_pseudo_id, session_id, join_key) AS enriched_product_class,
    MAX(super_class)      OVER (PARTITION BY user_pseudo_id, session_id, join_key) AS enriched_super_class,
    MAX(line_of_business) OVER (PARTITION BY user_pseudo_id, session_id, join_key) AS enriched_lob,
    MAX(item_catalog)     OVER (PARTITION BY user_pseudo_id, session_id, join_key) AS enriched_item_catalog
  FROM raw_events
),

session_segmentation AS (
  SELECT
    date,
    user_pseudo_id,
    session_id,
    MAX(market_id)              AS market_id,
    MAX(priority_region)        AS priority_region,
    MAX(enriched_lob)           AS line_of_business,
    MAX(enriched_item_catalog)  AS item_catalog,
    MAX(enriched_product_class) AS product_class,
    MAX(enriched_super_class)   AS super_class,
    MAX(enriched_sku)           AS item_sku,
    -- Segment Flags
    TRUE AS is_overall,
    LOGICAL_OR(event_name = 'view_search_results' OR REGEXP_CONTAINS(page_path, r"/search/"))             AS is_search,
    LOGICAL_OR(REGEXP_CONTAINS(page_path, r"^(.*/)?categories/[^/]+/?$"))                                  AS is_pcp,
    LOGICAL_OR(REGEXP_CONTAINS(page_path, r"^(.*/)?categories/[^/]+/[^/]+/?$") OR REGEXP_CONTAINS(page_path, r"/Addons/")) AS is_pdp,
    LOGICAL_OR(REGEXP_CONTAINS(page_path, r"/my-account/"))                                               AS is_my_account,
    LOGICAL_OR(REGEXP_CONTAINS(page_path, r".*/solutions/.*"))                                             AS is_solutions,
    -- Funnel Flags
    LOGICAL_OR(event_name = 'add_to_cart')                                          AS has_atc,
    LOGICAL_OR(event_name = 'view_cart')                                            AS has_view_cart,
    LOGICAL_OR(event_name = 'begin_checkout')                                       AS has_checkout,
    LOGICAL_OR(transaction_id IS NOT NULL AND transaction_id != '(not set)')        AS has_purchase
  FROM enriched_events
  GROUP BY 1, 2, 3
),

-- Wide aggregation: one row per date + dimensions
-- Includes both session-level and user-level counts + computed rates
agg AS (
  SELECT
    date,
    IFNULL(market_id,       '(not set)') AS market_id,
    IFNULL(priority_region, '(not set)') AS priority_region,
    IFNULL(line_of_business,'(not set)') AS line_of_business,
    IFNULL(item_catalog,    '(not set)') AS item_catalog,
    IFNULL(product_class,   '(not set)') AS product_class,
    IFNULL(super_class,     '(not set)') AS super_class,
    IFNULL(item_sku,        '(not set)') AS item_sku,

    -- ========== SESSION-LEVEL COUNTS ==========

    -- OVERALL
    COUNT(DISTINCT IF(is_overall, session_id, NULL))                        AS overall_sessions,
    COUNT(DISTINCT IF(is_overall AND has_atc, session_id, NULL))            AS overall_atc_sessions,
    COUNT(DISTINCT IF(is_overall AND has_view_cart, session_id, NULL))      AS overall_view_cart_sessions,
    COUNT(DISTINCT IF(is_overall AND has_checkout, session_id, NULL))       AS overall_checkout_sessions,
    COUNT(DISTINCT IF(is_overall AND has_purchase, session_id, NULL))       AS overall_purchase_sessions,

    -- PDP
    COUNT(DISTINCT IF(is_pdp, session_id, NULL))                            AS pdp_sessions,
    COUNT(DISTINCT IF(is_pdp AND has_atc, session_id, NULL))                AS pdp_atc_sessions,
    COUNT(DISTINCT IF(is_pdp AND has_view_cart, session_id, NULL))          AS pdp_view_cart_sessions,
    COUNT(DISTINCT IF(is_pdp AND has_checkout, session_id, NULL))           AS pdp_checkout_sessions,
    COUNT(DISTINCT IF(is_pdp AND has_purchase, session_id, NULL))           AS pdp_purchase_sessions,

    -- SEARCH
    COUNT(DISTINCT IF(is_search, session_id, NULL))                         AS search_sessions,
    COUNT(DISTINCT IF(is_search AND has_atc, session_id, NULL))             AS search_atc_sessions,
    COUNT(DISTINCT IF(is_search AND has_view_cart, session_id, NULL))       AS search_view_cart_sessions,
    COUNT(DISTINCT IF(is_search AND has_checkout, session_id, NULL))        AS search_checkout_sessions,
    COUNT(DISTINCT IF(is_search AND has_purchase, session_id, NULL))        AS search_purchase_sessions,

    -- PCP
    COUNT(DISTINCT IF(is_pcp, session_id, NULL))                            AS pcp_sessions,
    COUNT(DISTINCT IF(is_pcp AND has_atc, session_id, NULL))                AS pcp_atc_sessions,
    COUNT(DISTINCT IF(is_pcp AND has_view_cart, session_id, NULL))          AS pcp_view_cart_sessions,
    COUNT(DISTINCT IF(is_pcp AND has_checkout, session_id, NULL))           AS pcp_checkout_sessions,
    COUNT(DISTINCT IF(is_pcp AND has_purchase, session_id, NULL))           AS pcp_purchase_sessions,

    -- MY ACCOUNT
    COUNT(DISTINCT IF(is_my_account, session_id, NULL))                     AS accounts_sessions,
    COUNT(DISTINCT IF(is_my_account AND has_atc, session_id, NULL))         AS accounts_atc_sessions,
    COUNT(DISTINCT IF(is_my_account AND has_view_cart, session_id, NULL))   AS accounts_view_cart_sessions,
    COUNT(DISTINCT IF(is_my_account AND has_checkout, session_id, NULL))    AS accounts_checkout_sessions,
    COUNT(DISTINCT IF(is_my_account AND has_purchase, session_id, NULL))    AS accounts_purchase_sessions,

    -- SOLUTIONS
    COUNT(DISTINCT IF(is_solutions, session_id, NULL))                      AS solutions_sessions,
    COUNT(DISTINCT IF(is_solutions AND has_atc, session_id, NULL))          AS solutions_atc_sessions,
    COUNT(DISTINCT IF(is_solutions AND has_view_cart, session_id, NULL))    AS solutions_view_cart_sessions,
    COUNT(DISTINCT IF(is_solutions AND has_checkout, session_id, NULL))     AS solutions_checkout_sessions,
    COUNT(DISTINCT IF(is_solutions AND has_purchase, session_id, NULL))     AS solutions_purchase_sessions,

    -- ========== USER-LEVEL COUNTS ==========

    -- OVERALL
    COUNT(DISTINCT IF(is_overall, user_pseudo_id, NULL))                    AS overall_users,
    COUNT(DISTINCT IF(is_overall AND has_atc, user_pseudo_id, NULL))        AS overall_atc_users,
    COUNT(DISTINCT IF(is_overall AND has_purchase, user_pseudo_id, NULL))   AS overall_purchase_users,

    -- PDP
    COUNT(DISTINCT IF(is_pdp, user_pseudo_id, NULL))                        AS pdp_users,
    COUNT(DISTINCT IF(is_pdp AND has_atc, user_pseudo_id, NULL))            AS pdp_atc_users,
    COUNT(DISTINCT IF(is_pdp AND has_purchase, user_pseudo_id, NULL))       AS pdp_purchase_users,

    -- SEARCH
    COUNT(DISTINCT IF(is_search, user_pseudo_id, NULL))                     AS search_users,
    COUNT(DISTINCT IF(is_search AND has_atc, user_pseudo_id, NULL))         AS search_atc_users,
    COUNT(DISTINCT IF(is_search AND has_purchase, user_pseudo_id, NULL))    AS search_purchase_users,

    -- PCP
    COUNT(DISTINCT IF(is_pcp, user_pseudo_id, NULL))                        AS pcp_users,
    COUNT(DISTINCT IF(is_pcp AND has_atc, user_pseudo_id, NULL))            AS pcp_atc_users,
    COUNT(DISTINCT IF(is_pcp AND has_purchase, user_pseudo_id, NULL))       AS pcp_purchase_users,

    -- MY ACCOUNT
    COUNT(DISTINCT IF(is_my_account, user_pseudo_id, NULL))                 AS accounts_users,
    COUNT(DISTINCT IF(is_my_account AND has_atc, user_pseudo_id, NULL))     AS accounts_atc_users,
    COUNT(DISTINCT IF(is_my_account AND has_purchase, user_pseudo_id, NULL)) AS accounts_purchase_users,

    -- SOLUTIONS
    COUNT(DISTINCT IF(is_solutions, user_pseudo_id, NULL))                  AS solutions_users,
    COUNT(DISTINCT IF(is_solutions AND has_atc, user_pseudo_id, NULL))      AS solutions_atc_users,
    COUNT(DISTINCT IF(is_solutions AND has_purchase, user_pseudo_id, NULL)) AS solutions_purchase_users

  FROM session_segmentation
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)

SELECT * FROM agg;
