SELECT country, iso3 FROM disasters_staging.stg_gdacs WHERE country IS NOT NULL LIMIT 5;

SELECT COUNT(*) FROM disasters_gold.gold_disasters;
