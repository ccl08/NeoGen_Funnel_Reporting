-- channel_ecomm_Vfinal: channel-level eCom funnel with session_facts dimensions.
-- Mirrors the pattern in pdp_sessions_Vfinal: pulls session-scoped attrs
-- (market_id, priority_region, line_of_business, logged_in_status) from
-- session_facts so they can be used as dashboard filters.
-- item_sku and item_catalog were intentionally excluded — session_facts
-- collapses them to one MAX value per session, which would mis-attribute
-- per-event metrics (view/ATC/purchase/revenue) when filtering.
-- Depends on: neogen-ga4-export.funnelPurchase_table.session_facts

CREATE OR REPLACE TABLE `neogen-ga4-export.reporting_tables.Channel_eCom_transactions_Vfinal` AS

WITH all_events AS (
    SELECT
        PARSE_DATE('%Y%m%d', event_date) AS date,
        user_pseudo_id,
        CONCAT(
            user_pseudo_id,
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        ) AS session_id,
        traffic_source.source AS source,
        traffic_source.medium AS medium,
        traffic_source.name   AS campaign,
        event_name,
        (SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'value') AS revenue_value -- only for purchase
    FROM `neogen-ga4-export.analytics_331328809.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '20250601' AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
        AND device.web_info.hostname = 'www.neogen.com'
        AND (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') IS NOT NULL
),

-- Enrich each event with session-level attributes pulled from session_facts.
enriched_events AS (
    SELECT
        e.date,
        e.user_pseudo_id,
        e.session_id,
        e.source,
        e.medium,
        e.campaign,
        e.event_name,
        e.revenue_value,
        IFNULL(s.market_id,        '(not set)') AS market_id,
        IFNULL(s.priority_region,  '(not set)') AS priority_region,
        IFNULL(s.line_of_business, '(not set)') AS line_of_business,
        IF(s.is_logged_in_session, 'true', 'false') AS logged_in_status
    FROM all_events e
    LEFT JOIN `neogen-ga4-export.funnelPurchase_table.session_facts` s
        USING (date, user_pseudo_id, session_id)
),

channel_grouping AS (
    SELECT
        medium_pattern,
        source_pattern,
        channel_group
    FROM `neogen-ga4-export.analytics_331328809.channel_mapping`
),

events_with_channel AS (
    SELECT
        e.date,
        e.user_pseudo_id,
        e.session_id,
        COALESCE(e.source,   'unknown') AS source,
        COALESCE(e.medium,   'unknown') AS medium,
        COALESCE(e.campaign, 'unknown') AS campaign,
        e.event_name,
        e.revenue_value,
        e.market_id,
        e.priority_region,
        e.line_of_business,
        e.logged_in_status,
        COALESCE(c.channel_group, 'Other') AS channel_group
    FROM enriched_events e
    LEFT JOIN channel_grouping c
        ON REGEXP_CONTAINS(LOWER(e.medium), c.medium_pattern)
        AND REGEXP_CONTAINS(LOWER(e.source), c.source_pattern)
),

aggregated_metrics AS (
    SELECT
        date,
        market_id,
        priority_region,
        line_of_business,
        logged_in_status,
        source,
        medium,
        campaign,
        channel_group,
        COUNT(DISTINCT user_pseudo_id) AS users,
        COUNT(DISTINCT CASE WHEN event_name = 'view_item_list' THEN user_pseudo_id END) AS view_item_list_users,
        COUNT(DISTINCT CASE WHEN event_name = 'view_item'      THEN user_pseudo_id END) AS view_item_users,
        COUNT(DISTINCT CASE WHEN event_name = 'add_to_cart'    THEN user_pseudo_id END) AS add_to_cart_users,
        COUNT(DISTINCT CASE WHEN event_name = 'purchase'       THEN user_pseudo_id END) AS purchase_users,
        SUM(CASE WHEN event_name = 'purchase' THEN COALESCE(revenue_value, 0) ELSE 0 END) AS revenue
    FROM events_with_channel
    GROUP BY
        date, market_id, priority_region, line_of_business,
        logged_in_status, source, medium, campaign, channel_group
)

SELECT *
FROM aggregated_metrics
ORDER BY date DESC, users DESC;
