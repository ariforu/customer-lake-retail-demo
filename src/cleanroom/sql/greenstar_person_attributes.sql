-- Greenstar Consumer Data (fictional third-party consumer data provider) - mock provider dataset.
-- Runs on the PROVIDER workspace (partner-demo-catalog). Placeholders: ${provider_catalog}, ${n_synthetic}.
-- Input: ${provider_catalog}.staging.seed_matches (email_sha256, country, birth_date, segment), written by
-- scripts/setup_provider.py from a sample of StrideForge customers so that some provider records match.
-- Every value is synthetic and derived deterministically from the email hash (re-runs give the same data).

CREATE SCHEMA IF NOT EXISTS ${provider_catalog}.consumer
COMMENT 'Greenstar Consumer Data (fictional provider, synthetic data): person-level consumer attributes licensed to clients through Databricks Clean Rooms. Detailed records never leave this schema; the clean room notebook returns summaries.'
;
CREATE TABLE IF NOT EXISTS ${provider_catalog}.consumer.person_attributes (
  person_id STRING NOT NULL COMMENT 'Greenstar person id (GS + 10 digits). Never returned to clients.',
  email_sha256 STRING NOT NULL COMMENT 'SHA-256 hex of the lower-cased, trimmed email address. The only match key.',
  country STRING COMMENT 'Country code: US, UK, DE, CA.',
  date_of_birth DATE COMMENT 'Exact date of birth (detailed; clients receive only generation / age band).',
  phone_number STRING COMMENT 'Verified mobile phone number in E.164 format.',
  household_income INT COMMENT 'Modelled annual household income in USD-equivalent (detailed; clients receive an income band).',
  home_ownership STRING COMMENT 'own or rent.',
  home_value INT COMMENT 'Modelled home value in USD-equivalent; null for renters (detailed).',
  occupation_title STRING COMMENT 'Self-reported or modelled occupation title (detailed; clients receive an occupation category).',
  education STRING COMMENT 'Highest education level: High School, Some College, Bachelor, Graduate.',
  marital_status STRING COMMENT 'single, married, divorced or widowed.',
  num_children INT COMMENT 'Number of children under 18 in the household.',
  interest_scores MAP<STRING, DOUBLE> COMMENT 'Affinity score 0-1 for 20 interest categories (detailed; clients receive the top 3 labels).',
  last_verified_date DATE COMMENT 'When Greenstar last verified the record.',
  CONSTRAINT pk_person_attributes PRIMARY KEY (person_id)
) COMMENT 'Greenstar Consumer Data (fictional provider, synthetic): one row per person with detailed consumer attributes keyed by hashed email. Shared into clean rooms; clients never read it directly.'
;
INSERT OVERWRITE ${provider_catalog}.consumer.person_attributes
WITH matched AS (
  SELECT email_sha256, country, birth_date AS client_birth_date, segment FROM ${provider_catalog}.staging.seed_matches
),
synthetic AS (
  SELECT sha2(concat('greenstar-synthetic-', id), 256) AS email_sha256,
         element_at(array('US','US','US','US','US','UK','UK','DE','DE','CA'),
                    cast(conv(substr(sha2(concat('c', id), 256), 1, 6), 16, 10) % 10 AS INT) + 1) AS country,
         CAST(NULL AS DATE) AS client_birth_date, 'unmatched' AS segment
  FROM range(${n_synthetic})
),
gs_base AS (SELECT * FROM matched UNION ALL SELECT * FROM synthetic),
gs_rand AS (
-- u_<k>: deterministic uniform(0,1) numbers from the email hash.
SELECT b.*,
  conv(substr(sha2(concat(email_sha256, 'dob'), 256), 1, 8), 16, 10) / 4294967295.0 AS u_dob,
  conv(substr(sha2(concat(email_sha256, 'shift'), 256), 1, 8), 16, 10) / 4294967295.0 AS u_shift,
  conv(substr(sha2(concat(email_sha256, 'phone'), 256), 1, 12), 16, 10) AS n_phone,
  conv(substr(sha2(concat(email_sha256, 'hasph'), 256), 1, 8), 16, 10) / 4294967295.0 AS u_has_phone,
  conv(substr(sha2(concat(email_sha256, 'hasdob'), 256), 1, 8), 16, 10) / 4294967295.0 AS u_has_dob,
  conv(substr(sha2(concat(email_sha256, 'inc'), 256), 1, 8), 16, 10) / 4294967295.0 AS u_inc,
  conv(substr(sha2(concat(email_sha256, 'own'), 256), 1, 8), 16, 10) / 4294967295.0 AS u_own,
  conv(substr(sha2(concat(email_sha256, 'occ'), 256), 1, 8), 16, 10) / 4294967295.0 AS u_occ,
  conv(substr(sha2(concat(email_sha256, 'edu'), 256), 1, 8), 16, 10) / 4294967295.0 AS u_edu,
  conv(substr(sha2(concat(email_sha256, 'mar'), 256), 1, 8), 16, 10) / 4294967295.0 AS u_mar,
  conv(substr(sha2(concat(email_sha256, 'kid'), 256), 1, 8), 16, 10) / 4294967295.0 AS u_kid,
  conv(substr(sha2(concat(email_sha256, 'ver'), 256), 1, 8), 16, 10) / 4294967295.0 AS u_ver
FROM gs_base b
),
gs_people AS (
SELECT r.*,
  -- Date of birth: the client's own value when it has one (5% are 1-3 years off, to show conflicts),
  -- otherwise a modelled age 18-80. About 8% of records have no date of birth.
  CASE
    WHEN client_birth_date IS NOT NULL AND u_shift < 0.05
      THEN add_months(client_birth_date, CAST((CASE WHEN u_shift < 0.025 THEN 1 ELSE -1 END) * (12 + floor(u_dob * 25)) AS INT))
    WHEN client_birth_date IS NOT NULL THEN client_birth_date
    WHEN u_has_dob < 0.08 THEN NULL
    ELSE date_sub(DATE'2026-10-31', CAST(365.25 * (18 + 62 * pow(u_dob, 1.3)) AS INT))
  END AS date_of_birth
FROM gs_rand r
),
gs_final AS (
SELECT p.*,
  floor(datediff(DATE'2026-10-31', coalesce(date_of_birth, DATE'1985-06-15')) / 365.25) AS age
FROM gs_people p
)
SELECT
  concat('GS', lpad(CAST(conv(substr(sha2(concat(email_sha256, 'pid'), 256), 1, 10), 16, 10) % 10000000000 AS STRING), 10, '0')) AS person_id,
  email_sha256,
  country,
  date_of_birth,
  -- About 12% have no phone. Numbers follow each country's mobile format.
  CASE WHEN u_has_phone < 0.12 THEN NULL
       WHEN country IN ('US', 'CA') THEN concat('+1', CAST(201 + n_phone % 788 AS STRING), lpad(CAST(floor(n_phone / 1000) % 10000000 AS STRING), 7, '0'))
       WHEN country = 'UK' THEN concat('+447', lpad(CAST(n_phone % 1000000000 AS STRING), 9, '0'))
       ELSE concat('+491', element_at(array('5', '6', '7'), CAST(n_phone % 3 AS INT) + 1), lpad(CAST(floor(n_phone / 3) % 1000000000 AS STRING), 9, '0'))
  END AS phone_number,
  CAST(round(15000 + 235000 * pow(u_inc, 1.8), -2) AS INT) AS household_income,
  CASE WHEN u_own < least(0.85, greatest(0.15, (age - 22) / 40.0)) THEN 'own' ELSE 'rent' END AS home_ownership,
  CASE WHEN u_own < least(0.85, greatest(0.15, (age - 22) / 40.0))
       THEN CAST(round(120000 + 880000 * pow(u_inc, 1.5) * (0.6 + 0.8 * u_own), -3) AS INT) END AS home_value,
  CASE
    WHEN age < 23 AND u_occ < 0.7 THEN 'Student'
    WHEN age >= 67 AND u_occ < 0.85 THEN 'Retired'
    ELSE element_at(array('Software Engineer', 'Accountant', 'Attorney', 'Physician', 'Marketing Manager', 'Financial Analyst',
                          'Registered Nurse', 'Teacher', 'Physical Therapist', 'Electrician', 'Plumber', 'Carpenter',
                          'Retail Associate', 'Barista', 'Restaurant Server', 'Small Business Owner', 'Consultant',
                          'Graphic Designer', 'Sales Representative', 'Operations Manager'),
                    CAST(floor(u_occ * 20) AS INT) + 1)
  END AS occupation_title,
  CASE WHEN u_edu < 0.28 THEN 'High School' WHEN u_edu < 0.55 THEN 'Some College'
       WHEN u_edu < 0.85 THEN 'Bachelor' ELSE 'Graduate' END AS education,
  CASE WHEN age < 27 AND u_mar < 0.8 THEN 'single'
       WHEN u_mar < 0.55 THEN 'married' WHEN u_mar < 0.8 THEN 'single'
       WHEN u_mar < 0.93 OR age < 60 THEN 'divorced' ELSE 'widowed' END AS marital_status,
  CASE WHEN age < 22 OR age > 62 THEN 0
       WHEN u_kid < 0.45 THEN 0 WHEN u_kid < 0.65 THEN 1 WHEN u_kid < 0.88 THEN 2 ELSE 3 END AS num_children,
  -- 20 interest affinities. StrideForge customers lean towards running and fitness (they shop at a running retailer).
  map_from_arrays(
    array('running', 'trail_running', 'hiking', 'cycling', 'yoga', 'gym_fitness', 'team_sports', 'outdoor_camping',
          'travel', 'cooking', 'gaming', 'music', 'fashion', 'home_garden', 'pets', 'tech_gadgets',
          'health_wellness', 'parenting', 'finance_investing', 'sustainability'),
    transform(sequence(0, 19), i ->
      round(least(1.0, conv(substr(sha2(concat(email_sha256, 'int', CAST(i AS STRING)), 256), 1, 8), 16, 10) / 4294967295.0
                       * 0.85 + CASE WHEN segment <> 'unmatched' AND i IN (0, 1, 5, 16) THEN 0.25 ELSE 0.0 END), 2))
  ) AS interest_scores,
  date_sub(DATE'2026-10-31', CAST(u_ver * 540 AS INT)) AS last_verified_date
FROM gs_final
;
DROP TABLE IF EXISTS ${provider_catalog}.staging.seed_matches
;
DROP SCHEMA IF EXISTS ${provider_catalog}.staging
