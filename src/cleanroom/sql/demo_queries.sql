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

-- 3. Before and after for one customer (StrideForge value, value after enrichment, source).
SELECT stack(9,
  'Name',             concat(p.first_name, ' ', p.last_name), concat(p.first_name, ' ', p.last_name), 'first_party',
  'City',             p.city,                                 p.city,                                 'first_party',
  'Phone',            p.phone_e164,                           e.phone_e164,                           e.phone_source,
  'Age band',         p.age_band,                             e.age_band,                             e.age_band_source,
  'Generation',       CAST(NULL AS STRING),                   e.generation,                           e.generation_source,
  'Income band',      CAST(NULL AS STRING),                   e.income_band,                          'greenstar',
  'Homeowner',        CAST(NULL AS STRING),                   CAST(e.homeowner AS STRING),            'greenstar',
  'Life stage',       CAST(NULL AS STRING),                   e.life_stage,                           'greenstar',
  'Top interests',    CAST(NULL AS STRING),                   array_join(e.top_interests, ', '),      'greenstar'
) AS (field, before_clean_room, after_clean_room, source)
FROM vijay_catalog.retail_gold.customer_profile p
JOIN vijay_catalog.retail_cdp.customer_enrichment e USING (unified_customer_id)
WHERE unified_customer_id = 'UC03465B308277';

-- 4. An audience that only exists because of the enrichment.
SELECT generation, count(*) AS customers
FROM vijay_catalog.retail_cdp.customer_enrichment
WHERE greenstar_matched AND homeowner AND phone_source = 'greenstar'
GROUP BY generation ORDER BY customers DESC;
