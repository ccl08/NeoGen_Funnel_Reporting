-- Aggregate session_facts into wide KPI table (one row per date + dimensions).
-- Depends on: neogen-ga4-export.funnelPurchase_table.session_facts
-- Run AFTER 01_session_facts.sql.

CREATE OR REPLACE TABLE `neogen-ga4-export.funnelPurchase_table.segment_funnel_kpis_final_v2` AS
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

  -- LOGGED IN (session had logged_in_status = 'true' at any event)
  COUNT(DISTINCT IF(is_logged_in_session, session_id, NULL))                         AS logged_in_sessions,
  COUNT(DISTINCT IF(is_logged_in_session AND has_atc, session_id, NULL))             AS logged_in_atc_sessions,
  COUNT(DISTINCT IF(is_logged_in_session AND has_view_cart, session_id, NULL))       AS logged_in_view_cart_sessions,
  COUNT(DISTINCT IF(is_logged_in_session AND has_checkout, session_id, NULL))        AS logged_in_checkout_sessions,
  COUNT(DISTINCT IF(is_logged_in_session AND has_purchase, session_id, NULL))        AS logged_in_purchase_sessions,

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
  COUNT(DISTINCT IF(is_solutions AND has_purchase, user_pseudo_id, NULL)) AS solutions_purchase_users,

  -- LOGGED IN (user had at least one session with logged_in_status = 'true')
  COUNT(DISTINCT IF(is_logged_in_session, user_pseudo_id, NULL))                     AS logged_in_users,
  COUNT(DISTINCT IF(is_logged_in_session AND has_atc, user_pseudo_id, NULL))         AS logged_in_atc_users,
  COUNT(DISTINCT IF(is_logged_in_session AND has_purchase, user_pseudo_id, NULL))    AS logged_in_purchase_users

FROM `neogen-ga4-export.funnelPurchase_table.session_facts`
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;
