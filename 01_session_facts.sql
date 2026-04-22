-- Build session-level facts table (one row per session).
-- Source of truth for downstream KPI aggregations and other session analyses.

CREATE TEMP FUNCTION GetStringParam(params ANY TYPE, param_key STRING) AS (
  (SELECT value.string_value FROM UNNEST(params) WHERE key = param_key LIMIT 1)
);
CREATE TEMP FUNCTION GetIntParam(params ANY TYPE, param_key STRING) AS (
  (SELECT value.int_value FROM UNNEST(params) WHERE key = param_key LIMIT 1)
);

CREATE OR REPLACE TABLE `neogen-ga4-export.funnelPurchase_table.session_facts`
PARTITION BY date
AS

WITH raw_events AS (
  SELECT
    PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS date,
    user_pseudo_id,
    event_name,
    event_timestamp,
    CONCAT(user_pseudo_id, CAST(GetIntParam(event_params, 'ga_session_id') AS STRING)) AS session_id,
    REGEXP_REPLACE(GetStringParam(event_params, 'page_location'), r'(\?|#).*', '') AS join_key,
    IFNULL(REGEXP_EXTRACT(GetStringParam(event_params, 'page_location'), r"https?://[^/]+([^?#]*)"), '/') AS page_path,
    GetStringParam(event_params, 'market_id') AS market_id,
    GetStringParam(event_params, 'priority_region') AS priority_region,
    GetStringParam(event_params, 'logged_in_status') AS logged_in_status,
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
      'page_view', 'screen_view', 'view_search_results',
      'add_to_cart', 'view_cart', 'begin_checkout',
      'purchase', 'view_item', 'view_item_list'
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
)

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
  -- Segment flags
  TRUE AS is_overall,
  LOGICAL_OR(event_name = 'view_search_results' OR REGEXP_CONTAINS(page_path, r"/search/"))                                   AS is_search,
  LOGICAL_OR(REGEXP_CONTAINS(page_path, r"^(.*/)?categories/[^/]+/?$"))                                                        AS is_pcp,
  LOGICAL_OR(REGEXP_CONTAINS(page_path, r"^(.*/)?categories/[^/]+/[^/]+/?$") OR REGEXP_CONTAINS(page_path, r"/Addons/"))       AS is_pdp,
  LOGICAL_OR(REGEXP_CONTAINS(page_path, r"/my-account/"))                                                                      AS is_my_account,
  LOGICAL_OR(REGEXP_CONTAINS(page_path, r".*/solutions/.*"))                                                                   AS is_solutions,
  -- Funnel flags
  LOGICAL_OR(event_name = 'add_to_cart')                                          AS has_atc,
  LOGICAL_OR(event_name = 'view_cart')                                            AS has_view_cart,
  LOGICAL_OR(event_name = 'begin_checkout')                                       AS has_checkout,
  LOGICAL_OR(transaction_id IS NOT NULL AND transaction_id != '(not set)')        AS has_purchase,
  -- Logged-in flag: TRUE if user was logged in at any event during the session
  LOGICAL_OR(logged_in_status = 'true')                                           AS is_logged_in_session
FROM enriched_events
GROUP BY 1, 2, 3;
