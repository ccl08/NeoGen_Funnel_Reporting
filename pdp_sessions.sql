CREATE OR REPLACE TABLE `neogen-ga4-export.reporting_tables.pdp_sessions` AS

WITH all_events AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS date,
    user_pseudo_id,
    event_name,
    event_timestamp,
    CAST((SELECT value.int_value    FROM UNNEST(event_params) WHERE key = 'ga_session_id')    AS STRING) AS ga_session_id,
         (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location')              AS page_location,
         (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'market_id')                  AS market_id,
         (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'priority_region')            AS priority_region
  FROM `neogen-ga4-export.analytics_331328809.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20250601' AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND event_name IN ('page_view', 'screen_view', 'add_to_cart')
),

-- Classify pages with brand and category information
page_classification AS (
  SELECT
    *,
    REGEXP_REPLACE(page_location, r"^https?://[^/]+", "") AS full_path,

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

    -- Category label
    CASE
      WHEN REGEXP_CONTAINS(page_location, r"/categories/") THEN
        CASE
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "adulteration"                               THEN "Adulteration"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "allergens"                                  THEN "Allergens"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "dairy-residues"                             THEN "Dairy Residues"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "diagnostic-laboratory-services"             THEN "Laboratory Services"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "life-science-research"                      THEN "Life Science Research"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "microbiology"                               THEN "Microbiology"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "mycotoxins"                                 THEN "Mycotoxins"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "pathogens"                                  THEN "Pathogens"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "reagents-immunoassays"                      THEN "Reagents for Immunoassays"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "seafood-testing"                            THEN "Seafood Testing"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "toxicology"                                 THEN "Toxicology"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "veterinary-diagnostics"                     THEN "Veterinary Diagnostics"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "bacterial-sequencing"                       THEN "Bacterial Sequencing"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "companion-animal-genetic-traits-conditions" THEN "Companion Animal Genetic Traits & Conditions"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "dna-sequencing"                             THEN "DNA Sequencing"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "genotyping-arrays"                          THEN "Genotyping Arrays"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "igenity-profiles"                           THEN "Igenity Profiles"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "livestock-genetic-traits-conditions"        THEN "Livestock Genetic Traits & Conditions"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "parentage"                                  THEN "Parentage"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "sampling-equipment"                         THEN "Sampling Equipment"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "traceability-blockchain-applications"       THEN "Traceability & Blockchain Applications"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "animal-health"                              THEN "Animal Health"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "environmental-monitoring"                   THEN "Environmental Monitoring"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "insect-control"                             THEN "Insect Control"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "rodent-control"                             THEN "Rodent Control"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "sanitation-hygiene"                         THEN "Sanitation & Hygiene"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "veterinary-instruments"                     THEN "Veterinary Instruments"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "water-treatment"                            THEN "Water Treatment"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "carbohydrate-research"                      THEN "Carbohydrate Research"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "diagnostic-research-enzymes"                THEN "Diagnostic & Research Enzymes"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "enzyme-activity-analysis"                   THEN "Enzyme Activity Analysis"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "food-quality-control"                       THEN "Food Quality Control"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") = "nutritional-analysis"                       THEN "Nutritional Analysis"
          ELSE "Other"
        END
      ELSE "Other"
    END AS category_lookup,

    -- Page type: "category" for category landing, otherwise last path segment
    CASE
      WHEN REGEXP_CONTAINS(page_location, r"/categories/[^/]+/?$")  THEN "category"
      WHEN REGEXP_CONTAINS(page_location, r"/categories/.*/.*/?$") THEN REGEXP_EXTRACT(page_location, r"/categories/.*/([^/]+)/?$")
      ELSE "Other"
    END AS page_type,

    -- Master category: groups categories into high-level buckets
    CASE
      WHEN REGEXP_CONTAINS(page_location, r"/categories/") THEN
        CASE
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") IN (
            "adulteration", "allergens", "dairy-residues", "diagnostic-laboratory-services",
            "life-science-research", "microbiology", "mycotoxins", "pathogens",
            "reagents-immunoassays", "seafood-testing", "toxicology", "veterinary-diagnostics"
          ) THEN "Diagnostics"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") IN (
            "bacterial-sequencing", "companion-animal-genetic-traits-conditions", "dna-sequencing",
            "genotyping-arrays", "igenity-profiles", "livestock-genetic-traits-conditions",
            "parentage", "sampling-equipment", "traceability-blockchain-applications"
          ) THEN "Genomics"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") IN (
            "animal-health", "environmental-monitoring", "insect-control", "rodent-control",
            "sanitation-hygiene", "veterinary-instruments", "water-treatment"
          ) THEN "Prevention & Mitigation"
          WHEN REGEXP_EXTRACT(page_location, r"/categories/([^/]+)/") IN (
            "carbohydrate-research", "diagnostic-research-enzymes", "enzyme-activity-analysis",
            "food-quality-control", "nutritional-analysis"
          ) THEN "Megazyme"
          ELSE "Other Category"
        END
      ELSE "Non-Category Page"
    END AS master_category
  FROM all_events
),

-- Sessions that added to cart
cart_sessions AS (
  SELECT DISTINCT CONCAT(user_pseudo_id, ga_session_id) AS session_id
  FROM page_classification
  WHERE event_name = 'add_to_cart'
),

-- Page views only (for conversion-rate calculation)
all_page_views AS (
  SELECT *
  FROM page_classification
  WHERE event_name IN ('page_view', 'screen_view')
)

-- Final analysis: page-level add-to-cart influence
SELECT
  date,
  market_id,
  priority_region,
  PDP_Brand,
  category_lookup,
  master_category,
  page_type,
  page_location,
  -- Raw metrics for Looker Studio
  COUNT(DISTINCT CONCAT(user_pseudo_id, ga_session_id)) AS total_sessions_on_page,
  COUNT(*)                                              AS total_page_views,
  -- Cart conversion metric
  COUNT(DISTINCT CASE
    WHEN CONCAT(user_pseudo_id, ga_session_id) IN (SELECT session_id FROM cart_sessions)
    THEN CONCAT(user_pseudo_id, ga_session_id)
  END) AS sessions_that_added_to_cart
FROM all_page_views
GROUP BY date, market_id, priority_region, PDP_Brand, category_lookup, master_category, page_type, page_location
ORDER BY date DESC, sessions_that_added_to_cart DESC;
