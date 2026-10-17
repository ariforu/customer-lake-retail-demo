-- Reusable standardization functions used by identity resolution and conformance

CREATE OR REPLACE FUNCTION ${catalog}.${silver}.fix_email_domain(d STRING) RETURNS STRING
COMMENT 'Corrects common email domain typos'
RETURN CASE d WHEN 'gmial.com' THEN 'gmail.com' WHEN 'yaho.com' THEN 'yahoo.com' WHEN 'hotmial.com' THEN 'hotmail.com'
  WHEN 'outlok.com' THEN 'outlook.com' WHEN 'icloud.co' THEN 'icloud.com' WHEN 'gmx.d' THEN 'gmx.de'
  WHEN 'wbe.de' THEN 'web.de' ELSE d END;

CREATE OR REPLACE FUNCTION ${catalog}.${silver}.norm_email(e STRING) RETURNS STRING
COMMENT 'Lower-case, trim, drop +tags, fix domain typos, remove gmail dots'
RETURN CASE WHEN e IS NULL OR instr(e, '@') = 0 THEN NULL ELSE concat(
  CASE WHEN ${catalog}.${silver}.fix_email_domain(split_part(lower(trim(e)), '@', 2)) = 'gmail.com'
       THEN replace(regexp_replace(split_part(lower(trim(e)), '@', 1), '[+].*$', ''), '.', '')
       ELSE regexp_replace(split_part(lower(trim(e)), '@', 1), '[+].*$', '') END,
  '@', ${catalog}.${silver}.fix_email_domain(split_part(lower(trim(e)), '@', 2))) END;

CREATE OR REPLACE FUNCTION ${catalog}.${silver}.norm_phone(p STRING) RETURNS STRING
COMMENT 'Normalize US/CA/UK/DE phone formats to E.164'
RETURN CASE
  WHEN p IS NULL OR trim(p) = '' THEN NULL
  WHEN trim(p) LIKE '+%' THEN concat('+', regexp_replace(p, '[^0-9]', ''))
  WHEN length(regexp_replace(p, '[^0-9]', '')) = 11 AND regexp_replace(p, '[^0-9]', '') LIKE '1%'
    THEN concat('+', regexp_replace(p, '[^0-9]', ''))
  WHEN length(regexp_replace(p, '[^0-9]', '')) = 10 AND regexp_replace(p, '[^0-9]', '') NOT LIKE '0%'
    THEN concat('+1', regexp_replace(p, '[^0-9]', ''))
  WHEN regexp_replace(p, '[^0-9]', '') LIKE '07%' AND length(regexp_replace(p, '[^0-9]', '')) = 11
    THEN concat('+44', substr(regexp_replace(p, '[^0-9]', ''), 2))
  WHEN regexp_replace(p, '[^0-9]', '') LIKE '01%' THEN concat('+49', substr(regexp_replace(p, '[^0-9]', ''), 2))
  ELSE NULL END;

CREATE OR REPLACE FUNCTION ${catalog}.${silver}.norm_name(s STRING) RETURNS STRING
COMMENT 'Lower-case letters only'
RETURN nullif(regexp_replace(lower(trim(s)), '[^\\p{L}]', ''), '');

CREATE OR REPLACE FUNCTION ${catalog}.${silver}.canon_first_name(s STRING) RETURNS STRING
COMMENT 'Maps common nicknames to canonical first names'
RETURN CASE ${catalog}.${silver}.norm_name(s)
  WHEN 'bob' THEN 'robert' WHEN 'bill' THEN 'william' WHEN 'liz' THEN 'elizabeth' WHEN 'kate' THEN 'katherine'
  WHEN 'mike' THEN 'michael' WHEN 'jen' THEN 'jennifer' WHEN 'chris' THEN 'christopher' WHEN 'matt' THEN 'matthew'
  WHEN 'joe' THEN 'joseph' WHEN 'dan' THEN 'daniel' WHEN 'jess' THEN 'jessica' WHEN 'tom' THEN 'thomas'
  WHEN 'tony' THEN 'anthony' WHEN 'alex' THEN 'alexander' WHEN 'sam' THEN 'samantha' WHEN 'nick' THEN 'nicholas'
  WHEN 'becky' THEN 'rebecca' WHEN 'ben' THEN 'benjamin' WHEN 'maggie' THEN 'margaret' WHEN 'jim' THEN 'james'
  WHEN 'rick' THEN 'richard' WHEN 'pat' THEN 'patricia' WHEN 'steph' THEN 'stephanie' WHEN 'andy' THEN 'andrew'
  WHEN 'jon' THEN 'jonathan' ELSE ${catalog}.${silver}.norm_name(s) END;

CREATE OR REPLACE FUNCTION ${catalog}.${silver}.std_street(s STRING) RETURNS STRING
COMMENT 'Standardized street line for address matching'
RETURN nullif(trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
  regexp_replace(lower(s), '[.,#]', ' '), ' street\\b', ' st'), ' avenue\\b', ' ave'), ' road\\b', ' rd'),
  ' drive\\b', ' dr'), ' boulevard\\b', ' blvd'), ' lane\\b', ' ln'), '\\s+', ' ')), '');

CREATE OR REPLACE FUNCTION ${catalog}.${silver}.postal_from_text(s STRING) RETURNS STRING
COMMENT 'Extracts a US/DE, UK or CA postal code from the end of an address string'
RETURN nullif(regexp_extract(s, '([A-Z]{1,2}[0-9]{1,2} [0-9][A-Z]{2}|[A-Z][0-9][A-Z] [0-9][A-Z][0-9]|[0-9]{5})$', 1), '');

CREATE OR REPLACE FUNCTION ${catalog}.${silver}.country_from_postal(p STRING) RETURNS STRING
COMMENT 'Infers country from postal code shape (5-digit is ambiguous US/DE -> NULL)'
RETURN CASE WHEN p RLIKE '^[A-Z][0-9][A-Z] [0-9][A-Z][0-9]$' THEN 'CA'
            WHEN p RLIKE '^[A-Z]{1,2}[0-9]{1,2} [0-9][A-Z]{2}$' THEN 'UK' ELSE NULL END;
