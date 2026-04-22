-- pdp_sessions_Vfinal: page-level funnel dimensions for PDP dashboard.
-- Joins session_facts for all session-scoped attrs (market, item dims, logged_in, etc.)
-- so this file stays focused on page-level classification (brand / category / page type).
-- Depends on: neogen-ga4-export.funnelPurchase_table.session_facts

CREATE OR REPLACE TABLE `neogen-ga4-export.reporting_tables.pdp_sessions_Vfinal` AS

WITH pdp_events AS (
  -- Per-event: just what session_facts does NOT have (page_location, event_name).
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS date,
    user_pseudo_id,
    CONCAT(
      user_pseudo_id,
      CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    ) AS session_id,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title')    AS page_title,
    event_name
  FROM `neogen-ga4-export.analytics_331328809.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20250601' AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND device.web_info.hostname = 'www.neogen.com'
    AND event_name IN ('page_view', 'screen_view', 'add_to_cart')
),

-- Enrich each event with session-level attributes pulled from session_facts.
enriched_events AS (
  SELECT
    e.date,
    e.page_location,
    e.page_title,
    e.session_id,
    e.user_pseudo_id,
    e.event_name,
    IFNULL(s.market_id,        '(not set)') AS market_id,
    IFNULL(s.priority_region,  '(not set)') AS priority_region,
    IFNULL(s.line_of_business, '(not set)') AS line_of_business,
    IFNULL(s.item_catalog,     '(not set)') AS item_catalog,
    IFNULL(s.product_class,    '(not set)') AS product_class,
    IFNULL(s.super_class,      '(not set)') AS super_class,
    IFNULL(s.item_sku,         '(not set)') AS item_sku,
    IF(s.is_logged_in_session, 'true', 'false') AS logged_in_status
  FROM pdp_events e
  LEFT JOIN `neogen-ga4-export.funnelPurchase_table.session_facts` s
    USING (date, user_pseudo_id, session_id)
),

-- Extract category_slug once so downstream classifiers can reuse it
-- (BigQuery doesn't allow referencing a same-SELECT alias).
events_with_slug AS (
  SELECT
    *,
    REGEXP_REPLACE(page_location, r"^https?://[^/]+", "") AS full_path,
    REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") AS category_slug
  FROM enriched_events
),

-- Classify pages: brand, category, page type, master category.
page_classification AS (
  SELECT
    *,

    -- Brand (from category subpath)
    CASE
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/petrifilm")   THEN "Petrifilm"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/igenity")     THEN "Igenity"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/reveal")      THEN "Reveal"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/clean-trace") THEN "Clean Trace"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/veratox")     THEN "Veratox"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/ideal")       THEN "Ideal"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/soleris")     THEN "Soleris"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/accupoint")   THEN "AccuPoint"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/prozap")      THEN "Prozap"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/surekill")    THEN "SureKill"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/ramik")       THEN "Ramik"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/raptor")      THEN "Raptor"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/colitag")     THEN "Colitag"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/synergize")   THEN "Synergize"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/accuclean")   THEN "AccuClean"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/monitormark") THEN "MonitorMark"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/alert")       THEN "Alert"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/havoc")       THEN "Havoc"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/companion")   THEN "COMPANION"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/biosentry")   THEN "BioSentry"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/aquaprime")   THEN "AquaPrime"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/k-blue")      THEN "K-Blue"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/peraside")    THEN "Peraside"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/cykill")      THEN "Cykill"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/decimax")     THEN "DeciMax"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/neoseek")     THEN "NeoSeek"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.*/infiniseek")  THEN "InfiniSEEK"
      WHEN REGEXP_CONTAINS(page_location, r"(?i)/categories/.+/.+")          THEN "Non-Brand"
      ELSE NULL
    END AS PDP_Brand,

    -- Category label (one lookup per slug)
    CASE category_slug
      WHEN "adulteration"                               THEN "Adulteration"
      WHEN "allergens"                                  THEN "Allergens"
      WHEN "dairy-residues"                             THEN "Dairy Residues"
      WHEN "diagnostic-laboratory-services"             THEN "Laboratory Services"
      WHEN "life-science-research"                      THEN "Life Science Research"
      WHEN "microbiology"                               THEN "Microbiology"
      WHEN "mycotoxins"                                 THEN "Mycotoxins"
      WHEN "pathogens"                                  THEN "Pathogens"
      WHEN "reagents-immunoassays"                      THEN "Reagents for Immunoassays"
      WHEN "seafood-testing"                            THEN "Seafood Testing"
      WHEN "toxicology"                                 THEN "Toxicology"
      WHEN "veterinary-diagnostics"                     THEN "Veterinary Diagnostics"
      WHEN "bacterial-sequencing"                       THEN "Bacterial Sequencing"
      WHEN "companion-animal-genetic-traits-conditions" THEN "Companion Animal Genetic Traits & Conditions"
      WHEN "dna-sequencing"                             THEN "DNA Sequencing"
      WHEN "genotyping-arrays"                          THEN "Genotyping Arrays"
      WHEN "igenity-profiles"                           THEN "Igenity Profiles"
      WHEN "livestock-genetic-traits-conditions"        THEN "Livestock Genetic Traits & Conditions"
      WHEN "parentage"                                  THEN "Parentage"
      WHEN "sampling-equipment"                         THEN "Sampling Equipment"
      WHEN "traceability-blockchain-applications"       THEN "Traceability & Blockchain Applications"
      WHEN "animal-health"                              THEN "Animal Health"
      WHEN "environmental-monitoring"                   THEN "Environmental Monitoring"
      WHEN "insect-control"                             THEN "Insect Control"
      WHEN "rodent-control"                             THEN "Rodent Control"
      WHEN "sanitation-hygiene"                         THEN "Sanitation & Hygiene"
      WHEN "veterinary-instruments"                     THEN "Veterinary Instruments"
      WHEN "water-treatment"                            THEN "Water Treatment"
      WHEN "carbohydrate-research"                      THEN "Carbohydrate Research"
      WHEN "diagnostic-research-enzymes"                THEN "Diagnostic & Research Enzymes"
      WHEN "enzyme-activity-analysis"                   THEN "Enzyme Activity Analysis"
      WHEN "food-quality-control"                       THEN "Food Quality Control"
      WHEN "nutritional-analysis"                       THEN "Nutritional Analysis"
      ELSE "Other"
    END AS category_lookup,

    -- Page type: "category" for category landing, otherwise last path segment
    CASE
      WHEN REGEXP_CONTAINS(page_location, r"/categories/[^/]+/?$") THEN "category"
      WHEN REGEXP_CONTAINS(page_location, r"/categories/.*/.*/?$") THEN REGEXP_EXTRACT(page_location, r"/categories/.*/([^/]+)/?$")
      ELSE "Other"
    END AS page_type,

    -- Master category (groups category slugs into high-level buckets)
    CASE
      WHEN category_slug IN (
        "adulteration", "allergens", "dairy-residues", "diagnostic-laboratory-services",
        "life-science-research", "microbiology", "mycotoxins", "pathogens",
        "reagents-immunoassays", "seafood-testing", "toxicology", "veterinary-diagnostics"
      ) THEN "Diagnostics"
      WHEN category_slug IN (
        "bacterial-sequencing", "companion-animal-genetic-traits-conditions", "dna-sequencing",
        "genotyping-arrays", "igenity-profiles", "livestock-genetic-traits-conditions",
        "parentage", "sampling-equipment", "traceability-blockchain-applications"
      ) THEN "Genomics"
      WHEN category_slug IN (
        "animal-health", "environmental-monitoring", "insect-control", "rodent-control",
        "sanitation-hygiene", "veterinary-instruments", "water-treatment"
      ) THEN "Prevention & Mitigation"
      WHEN category_slug IN (
        "carbohydrate-research", "diagnostic-research-enzymes", "enzyme-activity-analysis",
        "food-quality-control", "nutritional-analysis"
      ) THEN "Megazyme"
      WHEN category_slug IS NOT NULL THEN "Other Category"
      ELSE "Non-Category Page"
    END AS master_category
  FROM events_with_slug
),

-- Sessions that added to cart (for cart conversion metric)
cart_sessions AS (
  SELECT DISTINCT session_id
  FROM page_classification
  WHERE event_name = 'add_to_cart'
),

-- Page views only (drop add_to_cart; those were only used to build cart_sessions)
page_views AS (
  SELECT *
  FROM page_classification
  WHERE event_name IN ('page_view', 'screen_view')
)

-- Final aggregation: one row per (date × all filter dims × page_location × classifier dims)
SELECT
  date,
  market_id,
  priority_region,
  line_of_business,
  item_catalog,
  product_class,
  super_class,
  item_sku,
  logged_in_status,
  PDP_Brand,
  category_lookup,
  master_category,
  page_type,
  page_location,
  IFNULL(page_title, '(not set)') AS page_title,
  COUNT(DISTINCT session_id)                                              AS total_sessions_on_page,
  COUNT(*)                                                                AS total_page_views,
  COUNT(DISTINCT IF(session_id IN (SELECT session_id FROM cart_sessions),
                    session_id, NULL))                                    AS sessions_that_added_to_cart
FROM page_views
GROUP BY date, market_id, priority_region, line_of_business, item_catalog,
         product_class, super_class, item_sku, logged_in_status,
         PDP_Brand, category_lookup, master_category, page_type, page_location, page_title
ORDER BY date DESC, sessions_that_added_to_cart DESC;
