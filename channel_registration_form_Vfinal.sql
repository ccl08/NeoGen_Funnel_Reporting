-- channel_registration_form_Vfinal: registration funnel with session_facts dimensions.
-- Mirrors the channel_ecomm_Vfinal pattern: pulls session-scoped attrs
-- (market_id, priority_region, line_of_business, logged_in_status) from
-- session_facts so they can be used as dashboard filters.
-- Depends on: neogen-ga4-export.funnelPurchase_table.session_facts

CREATE OR REPLACE TABLE `neogen-ga4-export.reporting_tables.channel_grouping_registration_form_Vfinal` AS

WITH all_events AS (
    SELECT
        PARSE_DATE('%Y%m%d', event_date) AS date,
        traffic_source.source AS source,
        traffic_source.medium AS medium,
        traffic_source.name   AS campaign,
        user_pseudo_id,
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) AS ga_session_id,
        CONCAT(
            user_pseudo_id,
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        ) AS session_id,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') AS session_engaged,
        event_name
    FROM `neogen-ga4-export.analytics_331328809.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '20250601' AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
        AND device.web_info.hostname = 'www.neogen.com'
        AND (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') IS NOT NULL
),

session_start_events AS (
    SELECT
        date, source, medium, campaign, user_pseudo_id,
        COUNT(*) AS session_start_event_count
    FROM all_events
    WHERE event_name = 'session_start'
    GROUP BY date, source, medium, campaign, user_pseudo_id
),

channel_grouping AS (
    SELECT
        medium_pattern,
        source_pattern,
        channel_group
    FROM `neogen-ga4-export.analytics_331328809.channel_mapping`
),

traffic_sessions AS (
    SELECT
        date,
        COALESCE(source,   'unknown') AS source,
        COALESCE(medium,   'unknown') AS medium,
        COALESCE(campaign, 'unknown') AS campaign,
        user_pseudo_id,
        ga_session_id,
        session_id,
        MAX(CASE WHEN session_engaged = '1' THEN 1 ELSE 0 END) AS is_engaged_session
    FROM all_events
    GROUP BY date, source, medium, campaign, user_pseudo_id, ga_session_id, session_id
),

-- Enrich each session with attributes from session_facts.
sessions_enriched AS (
    SELECT
        t.*,
        IFNULL(s.market_id,        '(not set)') AS market_id,
        IFNULL(s.priority_region,  '(not set)') AS priority_region,
        IFNULL(s.line_of_business, '(not set)') AS line_of_business,
        IF(s.is_logged_in_session, 'true', 'false') AS logged_in_status
    FROM traffic_sessions t
    LEFT JOIN `neogen-ga4-export.funnelPurchase_table.session_facts` s
        USING (date, user_pseudo_id, session_id)
),

traffic_sessions_with_channel AS (
    SELECT
        e.*,
        COALESCE(c.channel_group, 'Other') AS channel_group
    FROM sessions_enriched e
    LEFT JOIN channel_grouping c
        ON REGEXP_CONTAINS(LOWER(e.medium), c.medium_pattern)
        AND REGEXP_CONTAINS(LOWER(e.source), c.source_pattern)
),

registration_events AS (
    SELECT
        user_pseudo_id,
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) AS ga_session_id,
        LOWER(TRIM((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'registration_step')))         AS step_value,
        LOWER(TRIM((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'registration_existing_cust'))) AS existing_cust_value
    FROM `neogen-ga4-export.analytics_331328809.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '20250601' AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
        AND device.web_info.hostname = 'www.neogen.com'
        AND event_name = 'registration_step'
        AND (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') IS NOT NULL
),

session_registration_flags AS (
    SELECT
        user_pseudo_id,
        ga_session_id,
        MAX(step_value = 'pageview')                                                       AS did_pageview,
        MAX(step_value = 'user info')                                                      AS did_user_info,
        MAX(step_value = 'country dropdown')                                               AS did_country_dropdown,
        MAX(step_value = 'existing customer' AND existing_cust_value IS NOT NULL)          AS did_existing_customer,
        MAX(step_value IN ('registration complete', 'registration_complete'))              AS did_registration_complete,
        MAX(step_value = 'existing customer' AND existing_cust_value = 'customer - true')  AS is_customer_true,
        MAX(step_value = 'existing customer' AND existing_cust_value = 'customer - false') AS is_customer_false
    FROM registration_events
    GROUP BY user_pseudo_id, ga_session_id
),

sessions_with_registration AS (
    SELECT
        t.*,
        COALESCE(r.did_pageview,              FALSE) AS did_pageview,
        COALESCE(r.did_registration_complete, FALSE) AS did_registration_complete,
        COALESCE(r.is_customer_true,          FALSE) AS is_customer_true,
        COALESCE(r.is_customer_false,         FALSE) AS is_customer_false
    FROM traffic_sessions_with_channel t
    LEFT JOIN session_registration_flags r
        ON t.user_pseudo_id = r.user_pseudo_id
        AND t.ga_session_id = r.ga_session_id
),

-- Final join to attach session_start_event_count
final_sessions AS (
    SELECT
        s.*,
        COALESCE(e.session_start_event_count, 0) AS session_start_event_count
    FROM sessions_with_registration s
    LEFT JOIN session_start_events e
        ON s.date           = e.date
        AND s.source        = e.source
        AND s.medium        = e.medium
        AND s.campaign      = e.campaign
        AND s.user_pseudo_id = e.user_pseudo_id
)

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
    -- Sessions
    COUNT(DISTINCT session_id)     AS sessions,
    SUM(is_engaged_session)        AS sessions_engaged,
    SUM(session_start_event_count) AS session_start_events,
    -- Registration funnel metrics
    COUNTIF(did_pageview)              AS sessions_with_form_start,
    COUNTIF(did_registration_complete) AS sessions_with_registration_complete,
    COUNTIF(is_customer_true)          AS sessions_with_customer_true,
    COUNTIF(is_customer_false)         AS sessions_with_customer_false
FROM final_sessions
GROUP BY
    date, market_id, priority_region, line_of_business, logged_in_status,
    source, medium, campaign, channel_group
ORDER BY date DESC, sessions DESC;
