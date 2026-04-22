-- 1. SCALABLE UDFs
CREATE TEMP FUNCTION GetStringParam(params ANY TYPE, param_key STRING) AS (
  (SELECT value.string_value FROM UNNEST(params) WHERE key = param_key LIMIT 1)
);
CREATE TEMP FUNCTION GetIntParam(params ANY TYPE, param_key STRING) AS (
  (SELECT value.int_value FROM UNNEST(params) WHERE key = param_key LIMIT 1)
);

-- 2. Build the Reporting Table
CREATE OR REPLACE TABLE `neogen-ga4-export.funnelPurchase_table.segment_funnel_kpis_final` AS

WITH raw_events AS (
  SELECT
    PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS date,
    user_pseudo_id,
    event_name,
    event_timestamp,
    -- Construct Session ID
    CONCAT(user_pseudo_id, CAST(GetIntParam(event_params, 'ga_session_id') AS STRING)) AS session_id,
    -- URL Processing
    GetStringParam(event_params, 'page_location') AS page_location,
    -- Join Key: Strips ?query=params for accurate internal data enrichment
    REGEXP_REPLACE(GetStringParam(event_params, 'page_location'), r'(\?|#).*', '') AS join_key,
    -- Page Path: Mirrored from Looker Studio "Page path and screen class"
    IFNULL(REGEXP_EXTRACT(GetStringParam(event_params, 'page_location'), r"https?://[^/]+([^?#]*)"), '/') AS page_path,
    -- Custom Event Dimensions
    GetStringParam(event_params, 'market_id') AS market_id,
    GetStringParam(event_params, 'priority_region') AS priority_region,
    GetStringParam(event_params, 'page_title') AS page_title,
    GetStringParam(event_params, 'logged_in_status') AS logged_in_status,
    -- Item-Scoped Dimensions (Flattened from Items Array)
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
    -- Include all necessary events for segments and funnel
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

-- 3. ENRICHMENT: Copy product data onto Page Views within the same session/URL
enriched_events AS (
  SELECT
    *,
    MAX(item_sku)       OVER (PARTITION BY user_pseudo_id, session_id, join_key) AS enriched_sku,
    MAX(product_class)  OVER (PARTITION BY user_pseudo_id, session_id, join_key) AS enriched_product_class,
    MAX(super_class)    OVER (PARTITION BY user_pseudo_id, session_id, join_key) AS enriched_super_class,
    MAX(line_of_business) OVER (PARTITION BY user_pseudo_id, session_id, join_key) AS enriched_lob,
    MAX(item_catalog)     OVER (PARTITION BY user_pseudo_id, session_id, join_key) AS enriched_item_catalog
  FROM raw_events
),

-- 4. SEGMENTATION: Apply your logic using Clean Page Paths
session_segmentation AS (
  SELECT
    date,
    user_pseudo_id,
    session_id,
    -- Global Filter Values (Assigned to the session)
    MAX(market_id)            AS market_id,
    MAX(priority_region)      AS priority_region,
    MAX(enriched_lob)         AS line_of_business,
    MAX(enriched_item_catalog) AS item_catalog,
    MAX(enriched_product_class) AS product_class,
    MAX(enriched_super_class) AS super_class,
    MAX(enriched_sku)         AS item_sku,
    MAX(page_title)           AS page_title,
    MAX(page_location)        AS page_location,
    -- Segment Logic (User touched this segment at any point in the session)
    TRUE AS is_overall,
    LOGICAL_OR(
      event_name = 'view_search_results'
      OR REGEXP_CONTAINS(page_path, r"/search/")
    ) AS is_search,
    LOGICAL_OR(REGEXP_CONTAINS(page_path, r"^(.*/)?categories/[^/]+/?$")) AS is_pcp,
    LOGICAL_OR(
      REGEXP_CONTAINS(page_path, r"^(.*/)?categories/[^/]+/[^/]+/?$")
      OR REGEXP_CONTAINS(page_path, r"/Addons/")
    ) AS is_pdp,
    LOGICAL_OR(REGEXP_CONTAINS(page_path, r"/my-account/")) AS is_my_account,
    LOGICAL_OR(REGEXP_CONTAINS(page_path, r".*/solutions/.*")) AS is_solutions,
    -- Funnel Logic
    LOGICAL_OR(event_name = 'view_item')      AS has_view_item,
    LOGICAL_OR(event_name = 'view_item_list') AS has_view_item_list,
    LOGICAL_OR(event_name = 'add_to_cart')    AS has_atc,
    LOGICAL_OR(event_name = 'begin_checkout') AS has_checkout,
    LOGICAL_OR(transaction_id IS NOT NULL AND transaction_id != '(not set)') AS has_purchase,
    -- Logged-in flag: TRUE if user was logged in at any event during the session
    LOGICAL_OR(logged_in_status = 'true') AS is_logged_in_session
  FROM enriched_events
  GROUP BY 1, 2, 3
),

-- 5. UNPIVOT: Create the "Tall" table for Looker Studio
unpivoted_segments AS (
  SELECT
    date,
    IFNULL(market_id,       '(not set)') AS market_id,
    IFNULL(priority_region, '(not set)') AS priority_region,
    IFNULL(line_of_business,'(not set)') AS line_of_business,
    IFNULL(item_catalog,   '(not set)') AS item_catalog,
    IFNULL(product_class,   '(not set)') AS product_class,
    IFNULL(super_class,     '(not set)') AS super_class,
    IFNULL(item_sku,        '(not set)') AS item_sku,
    IFNULL(page_title,      '(not set)') AS page_title,
    IFNULL(page_location,   '(not set)') AS page_location,
    user_pseudo_id,
    session_id,
    has_view_item,
    has_view_item_list,
    has_atc,
    has_checkout,
    has_purchase,
    IF(is_logged_in_session, 'true', 'false') AS logged_in_status,
    s.Segment
  FROM session_segmentation
  CROSS JOIN UNNEST([
    STRUCT('1. Overall'    AS Segment, is_overall    AS valid),
    STRUCT('2. Search'     AS Segment, is_search     AS valid),
    STRUCT('3. PCP'        AS Segment, is_pcp        AS valid),
    STRUCT('4. Product'    AS Segment, is_pdp        AS valid),
    STRUCT('5. My Account' AS Segment, is_my_account AS valid),
    STRUCT('6. Solutions'  AS Segment, is_solutions  AS valid)
  ]) AS s
  WHERE s.valid = TRUE
)

-- 6. FINAL AGGREGATION
SELECT
  date,
  market_id,
  priority_region,
  line_of_business,
  item_catalog,
  product_class,
  super_class,
  item_sku,
  Segment,
  logged_in_status,
  page_title,
  page_location,
  COUNT(DISTINCT user_pseudo_id)                                    AS total_users,
  COUNT(DISTINCT session_id)                                        AS sessions,
  COUNT(DISTINCT CASE WHEN has_view_item      THEN user_pseudo_id END) AS view_item_users,
  COUNT(DISTINCT CASE WHEN has_view_item_list THEN user_pseudo_id END) AS view_item_list_users,
  COUNT(DISTINCT CASE WHEN has_atc      THEN user_pseudo_id END)    AS atc_users,
  COUNT(DISTINCT CASE WHEN has_checkout THEN user_pseudo_id END)    AS checkout_users,
  COUNT(DISTINCT CASE WHEN has_purchase THEN user_pseudo_id END)    AS purchase_users
FROM unpivoted_segments
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12;
