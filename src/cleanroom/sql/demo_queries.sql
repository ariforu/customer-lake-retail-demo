-- Demo queries for the clean room enrichment demo (run on fevm-vijay, SQL editor, catalog vijay_catalog).

-- 1. The gap: StrideForge customers with no phone or no birth date.
SELECT count(*) AS customers,
       count_if(phone_e164 IS NULL) AS no_phone,
       count_if(birth_date IS NULL) AS no_birth_date
FROM vijay_catalog.retail_gold.customer_profile;

-- 2. What the clean room returned (one run).
SELECT count(*) AS matched_customers,
       count_if(array_contains(fields_enriched, 'phone_e164')) AS phone_added,
       count_if(array_contains(fields_enriched, 'age_band')) AS age_band_added,
       count(generation) AS with_generation
FROM vijay_catalog.retail_cleanroom.customer_enrichment;

-- 3. Before and after for one customer.
SELECT p.first_name, p.last_name, p.city,
       p.phone_e164 AS phone_before, p.age_band AS age_band_before,
       e.phone_e164 AS phone_after, e.phone_source, e.age_band AS age_band_after, e.generation,
       e.income_band, e.homeowner, e.life_stage, e.top_interests
FROM vijay_catalog.retail_gold.customer_profile p
JOIN vijay_catalog.retail_cdp.customer_enrichment e USING (unified_customer_id)
WHERE unified_customer_id = 'UC03465B308277';

-- 4. An audience that only exists because of the enrichment.
SELECT generation, count(*) AS customers
FROM vijay_catalog.retail_cdp.customer_enrichment
WHERE greenstar_matched AND homeowner AND phone_source = 'greenstar'
GROUP BY generation ORDER BY customers DESC;
