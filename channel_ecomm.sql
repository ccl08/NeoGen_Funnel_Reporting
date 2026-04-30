CREATE OR REPLACE TABLE `neogen-ga4-export.reporting_tables.Channel_eCom_transactions` AS

WITH all_events AS (
    SELECT
        PARSE_DATE('%Y%m%d', event_date) AS date,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'market_id') AS market_id,
        traffic_source.source AS source,
        traffic_source.medium AS medium,
        traffic_source.name AS campaign,
        user_pseudo_id,
        event_name,
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) AS ga_session_id,
        (SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'value') AS revenue_value -- only for purchase
    FROM `neogen-ga4-export.analytics_331328809.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '20250101' AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
        AND (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') IS NOT NULL
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
        e.market_id,
        COALESCE(e.source, 'unknown') AS source,
        COALESCE(e.medium, 'unknown') AS medium,
        COALESCE(e.campaign, 'unknown') AS campaign,
        e.user_pseudo_id,
        e.ga_session_id,
        e.event_name,
        e.revenue_value,
        COALESCE(c.channel_group, 'Other') AS channel_group
    FROM all_events e
    LEFT JOIN channel_grouping c
        ON REGEXP_CONTAINS(LOWER(e.medium), c.medium_pattern)
        AND REGEXP_CONTAINS(LOWER(e.source), c.source_pattern)
),

aggregated_metrics AS (
    SELECT
        date,
        market_id,
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
    GROUP BY date, market_id, source, medium, campaign, channel_group
)

SELECT *
FROM aggregated_metrics
ORDER BY date DESC, users DESC;
