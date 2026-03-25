#!/usr/bin/env python3
"""Generate deterministic Postgres seed SQL for load testing."""

from __future__ import annotations

import argparse
import hashlib
import math
from pathlib import Path


SQL_TEMPLATE = """-- auto-generated load test seed SQL
-- restaurants={restaurant_count}, restaurant_start_id={restaurant_start_id}
-- target: roughly menus~{target_menus}, reviews~{target_reviews} with tiered long-tail distribution
-- profile={profile}, local_center=({local_center_lat}, {local_center_lng}) {local_center_sigungu} {local_center_eupmyeondong}
-- member auto-fill target: {member_target_count}
-- minimum seed auto-fill:
--   member -> {member_target_count}
--   "group" >= {min_groups}
--   subgroup >= {min_subgroups}
--   keyword >= {min_keywords}
--   food_category >= {min_food_categories}

BEGIN;

DROP TABLE IF EXISTS lt_restaurant_seed CASCADE;
DROP TABLE IF EXISTS lt_member_pool CASCADE;
DROP TABLE IF EXISTS lt_group_pool CASCADE;
DROP TABLE IF EXISTS lt_subgroup_pool CASCADE;
DROP TABLE IF EXISTS lt_keyword_pool CASCADE;
DROP TABLE IF EXISTS lt_food_category_pool CASCADE;
DROP TABLE IF EXISTS lt_menu_category_seed CASCADE;
DROP TABLE IF EXISTS lt_review_seed CASCADE;
DROP TABLE IF EXISTS lt_review_keyword_seed CASCADE;
DROP TABLE IF EXISTS lt_member_favorite_seed CASCADE;
DROP TABLE IF EXISTS lt_subgroup_favorite_seed CASCADE;

DO $$
DECLARE
  v_missing_count integer;
  v_start_id bigint;
  v_template_member_id bigint;
BEGIN
  SELECT GREATEST(0, {member_target_count} - count(*)) INTO v_missing_count
  FROM member;

  IF v_missing_count <= 0 THEN
    RETURN;
  END IF;

  SELECT id INTO v_template_member_id
  FROM member
  ORDER BY id
  LIMIT 1;

  SELECT GREATEST(COALESCE(max(id), 0), {member_start_id} - 1) + 1 INTO v_start_id
  FROM member;

  IF v_template_member_id IS NULL THEN
    INSERT INTO member (
      id,
      created_at,
      updated_at,
      deleted_at,
      agreed_privacy_at,
      agreed_terms_at,
      last_login_at,
      role,
      status,
      nickname,
      introduction,
      profile_image_url,
      email
    )
    SELECT
      v_start_id + gs - 1,
      now(),
      now(),
      NULL,
      now(),
      now(),
      now(),
      'USER',
      'ACTIVE',
      format('loadtest_member_%s', lpad((v_start_id + gs - 1)::text, 8, '0')),
      'load-test seeded member',
      NULL,
      format('loadtest-member-%s@seed.local', v_start_id + gs - 1)
    FROM generate_series(1, v_missing_count) AS gs
    ON CONFLICT DO NOTHING;
  ELSE
    INSERT INTO member (
      id,
      created_at,
      updated_at,
      deleted_at,
      agreed_privacy_at,
      agreed_terms_at,
      last_login_at,
      role,
      status,
      nickname,
      introduction,
      profile_image_url,
      email
    )
    SELECT
      v_start_id + gs - 1,
      COALESCE(m.created_at, now()),
      now(),
      NULL,
      COALESCE(m.agreed_privacy_at, now()),
      COALESCE(m.agreed_terms_at, now()),
      now(),
      m.role,
      m.status,
      format('loadtest_member_%s', lpad((v_start_id + gs - 1)::text, 8, '0')),
      COALESCE(m.introduction, 'load-test seeded member'),
      m.profile_image_url,
      format('loadtest-member-%s@seed.local', v_start_id + gs - 1)
    FROM member m
    CROSS JOIN generate_series(1, v_missing_count) AS gs
    WHERE m.id = v_template_member_id
    ON CONFLICT DO NOTHING;
  END IF;
END $$;

DO $$
DECLARE
  v_group_missing_count integer;
  v_group_start_id bigint;
  v_group_member_start_id bigint;
  v_member_id bigint;
BEGIN
  SELECT GREATEST(0, {min_groups} - count(*)) INTO v_group_missing_count
  FROM "group";

  IF v_group_missing_count <= 0 THEN
    RETURN;
  END IF;

  SELECT id INTO v_member_id
  FROM member
  ORDER BY id
  LIMIT 1;

  IF v_member_id IS NULL THEN
    RAISE EXCEPTION 'load-test seed requires at least 1 member before group auto-fill';
  END IF;

  SELECT GREATEST(COALESCE(max(id), 0), {group_start_id} - 1) + 1 INTO v_group_start_id
  FROM "group";

  SELECT GREATEST(COALESCE(max(id), 0), {group_member_start_id} - 1) + 1 INTO v_group_member_start_id
  FROM group_member;

  INSERT INTO "group" (
    id, name, type, logo_image_url, address, detail_address, location,
    join_type, email_domain, status, deleted_at, created_at, updated_at
  )
  SELECT
    v_group_start_id + gs - 1,
    format('loadtest_group_%s', lpad((v_group_start_id + gs - 1)::text, 6, '0')),
    'UNOFFICIAL',
    NULL,
    '경기도 성남시 분당구',
    '삼평동',
    ST_GeomFromText('POINT(127.107058 37.402052)', 4326),
    'PASSWORD',
    NULL,
    'ACTIVE',
    NULL,
    now(),
    now()
  FROM generate_series(1, v_group_missing_count) AS gs
  ON CONFLICT DO NOTHING;

  INSERT INTO group_member (id, group_id, member_id, deleted_at, created_at)
  SELECT
    v_group_member_start_id + gs - 1,
    v_group_start_id + gs - 1,
    v_member_id,
    NULL,
    now()
  FROM generate_series(1, v_group_missing_count) AS gs
  ON CONFLICT DO NOTHING;
END $$;

DO $$
DECLARE
  v_subgroup_missing_count integer;
  v_subgroup_start_id bigint;
  v_subgroup_member_start_id bigint;
  v_member_id bigint;
  v_group_id bigint;
BEGIN
  SELECT GREATEST(0, {min_subgroups} - count(*)) INTO v_subgroup_missing_count
  FROM subgroup;

  IF v_subgroup_missing_count <= 0 THEN
    RETURN;
  END IF;

  SELECT id INTO v_member_id
  FROM member
  ORDER BY id
  LIMIT 1;

  SELECT id INTO v_group_id
  FROM "group"
  ORDER BY id
  LIMIT 1;

  IF v_member_id IS NULL OR v_group_id IS NULL THEN
    RAISE EXCEPTION 'load-test seed requires member/group before subgroup auto-fill';
  END IF;

  SELECT GREATEST(COALESCE(max(id), 0), {subgroup_start_id} - 1) + 1 INTO v_subgroup_start_id
  FROM subgroup;

  SELECT GREATEST(COALESCE(max(id), 0), {subgroup_member_start_id} - 1) + 1 INTO v_subgroup_member_start_id
  FROM subgroup_member;

  INSERT INTO subgroup (
    id, group_id, name, description, profile_image_url,
    join_type, join_password, status, member_count, deleted_at, created_at, updated_at
  )
  SELECT
    v_subgroup_start_id + gs - 1,
    v_group_id,
    format('loadtest_subgroup_%s', lpad((v_subgroup_start_id + gs - 1)::text, 6, '0')),
    'load-test seeded subgroup',
    NULL,
    'OPEN',
    NULL,
    'ACTIVE',
    1,
    NULL,
    now(),
    now()
  FROM generate_series(1, v_subgroup_missing_count) AS gs
  ON CONFLICT DO NOTHING;

  INSERT INTO subgroup_member (id, subgroup_id, member_id, deleted_at, created_at)
  SELECT
    v_subgroup_member_start_id + gs - 1,
    v_subgroup_start_id + gs - 1,
    v_member_id,
    NULL,
    now()
  FROM generate_series(1, v_subgroup_missing_count) AS gs
  ON CONFLICT DO NOTHING;
END $$;

DO $$
DECLARE
  v_keyword_missing_count integer;
  v_keyword_start_id bigint;
BEGIN
  SELECT GREATEST(0, {min_keywords} - count(*)) INTO v_keyword_missing_count
  FROM keyword;

  IF v_keyword_missing_count <= 0 THEN
    RETURN;
  END IF;

  SELECT GREATEST(COALESCE(max(id), 0), {keyword_start_id} - 1) + 1 INTO v_keyword_start_id
  FROM keyword;

  INSERT INTO keyword (id, type, name)
  SELECT
    v_keyword_start_id + gs - 1,
    (ARRAY['VISIT_PURPOSE', 'COMPANION_TYPE', 'WAITING_EXPERIENCE', 'POSITIVE_ASPECT'])[1 + ((gs - 1) % 4)],
    format('loadtest_keyword_%s', lpad((v_keyword_start_id + gs - 1)::text, 6, '0'))
  FROM generate_series(1, v_keyword_missing_count) AS gs
  ON CONFLICT DO NOTHING;
END $$;

DO $$
DECLARE
  v_food_category_missing_count integer;
  v_food_category_start_id bigint;
BEGIN
  SELECT GREATEST(0, {min_food_categories} - count(*)) INTO v_food_category_missing_count
  FROM food_category;

  IF v_food_category_missing_count <= 0 THEN
    RETURN;
  END IF;

  SELECT GREATEST(COALESCE(max(id), 0), {food_category_start_id} - 1) + 1 INTO v_food_category_start_id
  FROM food_category;

  INSERT INTO food_category (id, name)
  SELECT
    v_food_category_start_id + gs - 1,
    (ARRAY['한식', '일식', '중식', '양식', '분식', '카페', '아시안', '주점'])[1 + ((gs - 1) % 8)]
  FROM generate_series(1, v_food_category_missing_count) AS gs
  ON CONFLICT DO NOTHING;
END $$;

CREATE OR REPLACE FUNCTION pg_temp.lt_hash60(key text)
RETURNS bigint
LANGUAGE SQL
IMMUTABLE
AS $$
  SELECT ('x' || substr(md5(key), 1, 15))::bit(60)::bigint;
$$;

CREATE OR REPLACE FUNCTION pg_temp.lt_uniform(key text)
RETURNS numeric
LANGUAGE SQL
IMMUTABLE
AS $$
  SELECT (pg_temp.lt_hash60(key) % 1000000)::numeric / 999999.0;
$$;

CREATE OR REPLACE FUNCTION pg_temp.lt_centered(key text, scale numeric)
RETURNS numeric
LANGUAGE SQL
IMMUTABLE
AS $$
  SELECT (pg_temp.lt_uniform(key) - 0.5) * 2.0 * scale;
$$;

CREATE OR REPLACE FUNCTION pg_temp.lt_rand_int(
  key text,
  min_value integer,
  max_value integer,
  skew numeric DEFAULT 1.0
)
RETURNS integer
LANGUAGE SQL
IMMUTABLE
AS $$
  SELECT LEAST(
    max_value,
    min_value + floor(power(pg_temp.lt_uniform(key), skew) * (max_value - min_value + 1))::integer
  );
$$;

CREATE OR REPLACE FUNCTION pg_temp.lt_pick_idx(key text, size integer)
RETURNS integer
LANGUAGE SQL
IMMUTABLE
AS $$
  SELECT CASE
    WHEN size <= 0 THEN 0
    ELSE (pg_temp.lt_hash60(key) % size)::integer
  END;
$$;

CREATE OR REPLACE FUNCTION pg_temp.lt_pick_idx(key text, size bigint)
RETURNS integer
LANGUAGE SQL
IMMUTABLE
AS $$
  SELECT pg_temp.lt_pick_idx(key, size::integer);
$$;

CREATE TEMP TABLE lt_member_pool AS
SELECT id, row_number() OVER (ORDER BY id) - 1 AS idx
FROM member;

CREATE TEMP TABLE lt_group_pool AS
SELECT id, row_number() OVER (ORDER BY id) - 1 AS idx
FROM "group";

CREATE TEMP TABLE lt_subgroup_pool AS
SELECT id, row_number() OVER (ORDER BY id) - 1 AS idx
FROM subgroup;

CREATE TEMP TABLE lt_keyword_pool AS
SELECT id, row_number() OVER (ORDER BY id) - 1 AS idx
FROM keyword;

CREATE TEMP TABLE lt_food_category_pool AS
SELECT id, row_number() OVER (ORDER BY id) - 1 AS idx
FROM food_category;

DO $$
DECLARE
  v_member_count integer;
  v_group_count integer;
  v_subgroup_count integer;
  v_keyword_count integer;
  v_food_category_count integer;
BEGIN
  SELECT count(*) INTO v_member_count FROM lt_member_pool;
  SELECT count(*) INTO v_group_count FROM lt_group_pool;
  SELECT count(*) INTO v_subgroup_count FROM lt_subgroup_pool;
  SELECT count(*) INTO v_keyword_count FROM lt_keyword_pool;
  SELECT count(*) INTO v_food_category_count FROM lt_food_category_pool;

  IF v_member_count < {min_members} THEN
    RAISE EXCEPTION 'load-test seed requires at least {min_members} members, found %', v_member_count;
  END IF;

  IF v_group_count < {min_groups} THEN
    RAISE EXCEPTION 'load-test seed requires at least {min_groups} groups, found %', v_group_count;
  END IF;

  IF v_subgroup_count < {min_subgroups} THEN
    RAISE EXCEPTION 'load-test seed requires at least {min_subgroups} subgroups, found %', v_subgroup_count;
  END IF;

  IF v_keyword_count < {min_keywords} THEN
    RAISE EXCEPTION 'load-test seed requires at least {min_keywords} keywords, found %', v_keyword_count;
  END IF;

  IF v_food_category_count < {min_food_categories} THEN
    RAISE EXCEPTION 'load-test seed requires at least {min_food_categories} food categories, found %', v_food_category_count;
  END IF;
END $$;

CREATE TEMP TABLE lt_restaurant_seed AS
WITH params AS (
  SELECT
    {restaurant_count}::integer AS restaurant_count,
    {restaurant_start_id}::integer AS restaurant_start_id,
    '{profile}'::text AS profile,
    {local_center_lat}::numeric AS local_center_lat,
    {local_center_lng}::numeric AS local_center_lng,
    '{local_center_sido}'::text AS local_center_sido,
    '{local_center_sigungu}'::text AS local_center_sigungu,
    '{local_center_eupmyeondong}'::text AS local_center_eupmyeondong,
    {dense_cluster_ratio}::numeric AS dense_cluster_ratio,
    {dense_grid_step}::numeric AS dense_grid_step,
    {dense_jitter}::numeric AS dense_jitter,
    {hotspot_lat_spread}::numeric AS hotspot_lat_spread,
    {hotspot_lng_spread}::numeric AS hotspot_lng_spread,
    {mid_lat_spread}::numeric AS mid_lat_spread,
    {mid_lng_spread}::numeric AS mid_lng_spread,
    {edge_lat_spread}::numeric AS edge_lat_spread,
    {edge_lng_spread}::numeric AS edge_lng_spread
),
zone_center AS (
  SELECT *
  FROM (
    VALUES
      ('PANGYO_CORE', 'HOTSPOT', '경기도', '성남시 분당구', '삼평동', 37.40110::numeric, 127.11090::numeric),
      ('LOCAL_DENSE', 'HOTSPOT', '{local_center_sido}', '{local_center_sigungu}', '{local_center_eupmyeondong}', {local_center_lat}::numeric, {local_center_lng}::numeric),
      ('GANGNAM_CORE', 'HOTSPOT', '서울특별시', '강남구', '역삼동', 37.49820::numeric, 127.02860::numeric),
      ('JAMSIL_CORE', 'HOTSPOT', '서울특별시', '송파구', '잠실동', 37.51330::numeric, 127.10280::numeric),
      ('SEOLLEUNG_CORE', 'HOTSPOT', '서울특별시', '강남구', '대치동', 37.50580::numeric, 127.04880::numeric),
      ('JEONGJA', 'MID', '경기도', '성남시 분당구', '정자동', 37.36780::numeric, 127.10890::numeric),
      ('SEOHYEON', 'MID', '경기도', '성남시 분당구', '서현동', 37.38520::numeric, 127.12320::numeric),
      ('MUNJEONG', 'MID', '서울특별시', '송파구', '문정동', 37.48490::numeric, 127.12290::numeric),
      ('SEONGSU', 'MID', '서울특별시', '성동구', '성수동', 37.54460::numeric, 127.05570::numeric),
      ('GWANGGYO', 'MID', '경기도', '수원시 영통구', '이의동', 37.28760::numeric, 127.05680::numeric),
      ('JUKJEON', 'EDGE', '경기도', '용인시 수지구', '죽전동', 37.32430::numeric, 127.10810::numeric),
      ('MISA', 'EDGE', '경기도', '하남시', '망월동', 37.56320::numeric, 127.19000::numeric),
      ('DASAN', 'EDGE', '경기도', '남양주시', '다산동', 37.62460::numeric, 127.15140::numeric),
      ('GURAE', 'EDGE', '경기도', '김포시', '구래동', 37.64580::numeric, 126.62890::numeric),
      ('YEONGTONG', 'EDGE', '경기도', '수원시 영통구', '영통동', 37.25210::numeric, 127.07120::numeric)
  ) AS t(center_key, zone_bucket, sido, sigungu, eupmyeondong, base_lat, base_lng)
),
base AS (
  SELECT
    gs AS seq,
    p.restaurant_start_id + gs - 1 AS restaurant_id,
    p.profile,
    p.local_center_lat,
    p.local_center_lng,
    p.local_center_sido,
    p.local_center_sigungu,
    p.local_center_eupmyeondong,
    p.dense_cluster_ratio,
    p.dense_grid_step,
    p.dense_jitter,
    p.hotspot_lat_spread,
    p.hotspot_lng_spread,
    p.mid_lat_spread,
    p.mid_lng_spread,
    p.edge_lat_spread,
    p.edge_lng_spread,
    CASE
      WHEN gs <= floor(p.restaurant_count * 0.70) THEN 'LONG_TAIL'
      WHEN gs <= floor(p.restaurant_count * 0.90) THEN 'MID_POPULAR'
      WHEN gs <= floor(p.restaurant_count * 0.98) THEN 'HOTSPOT_POPULAR'
      ELSE 'TOP_DENSE'
    END AS tier,
    CASE
      WHEN p.profile = 'local-dense' THEN 'HOTSPOT'
      WHEN gs <= floor(p.restaurant_count * 0.45) THEN 'HOTSPOT'
      WHEN gs <= floor(p.restaurant_count * 0.80) THEN 'MID'
      ELSE 'EDGE'
    END AS zone_bucket
  FROM params p
  CROSS JOIN generate_series(1, p.restaurant_count) AS gs
),
assigned AS (
  SELECT
    b.*,
    CASE
      WHEN b.profile = 'local-dense' THEN 'LOCAL_DENSE'
      WHEN b.zone_bucket = 'HOTSPOT' THEN
        (ARRAY['PANGYO_CORE', 'GANGNAM_CORE', 'JAMSIL_CORE', 'SEOLLEUNG_CORE'])[1 + pg_temp.lt_pick_idx('center:' || b.seq::text, 4)]
      WHEN b.zone_bucket = 'MID' THEN
        (ARRAY['JEONGJA', 'SEOHYEON', 'MUNJEONG', 'SEONGSU', 'GWANGGYO'])[1 + pg_temp.lt_pick_idx('center:' || b.seq::text, 5)]
      ELSE
        (ARRAY['JUKJEON', 'MISA', 'DASAN', 'GURAE', 'YEONGTONG'])[1 + pg_temp.lt_pick_idx('center:' || b.seq::text, 5)]
    END AS center_key
  FROM base b
),
computed AS (
  SELECT
    a.seq,
    a.restaurant_id,
    a.tier,
    a.zone_bucket,
    z.center_key,
    CASE WHEN a.profile = 'local-dense' THEN a.local_center_sido ELSE z.sido END AS sido,
    CASE WHEN a.profile = 'local-dense' THEN a.local_center_sigungu ELSE z.sigungu END AS sigungu,
    CASE WHEN a.profile = 'local-dense' THEN a.local_center_eupmyeondong ELSE z.eupmyeondong END AS eupmyeondong,
    CASE
      WHEN pg_temp.lt_uniform('dense:' || a.seq::text) < a.dense_cluster_ratio THEN
        (CASE WHEN a.profile = 'local-dense' THEN a.local_center_lat ELSE z.base_lat END)
        + ((pg_temp.lt_pick_idx('slotlat:' || a.seq::text, 15) - 7)::numeric * a.dense_grid_step)
        + pg_temp.lt_centered('jitlat:' || a.seq::text, a.dense_jitter)
      WHEN a.zone_bucket = 'HOTSPOT' THEN
        (CASE WHEN a.profile = 'local-dense' THEN a.local_center_lat ELSE z.base_lat END)
        + pg_temp.lt_centered('lat:' || a.seq::text, a.hotspot_lat_spread)
      WHEN a.zone_bucket = 'MID' THEN
        (CASE WHEN a.profile = 'local-dense' THEN a.local_center_lat ELSE z.base_lat END)
        + pg_temp.lt_centered('lat:' || a.seq::text, a.mid_lat_spread)
      ELSE
        (CASE WHEN a.profile = 'local-dense' THEN a.local_center_lat ELSE z.base_lat END)
        + pg_temp.lt_centered('lat:' || a.seq::text, a.edge_lat_spread)
    END AS lat,
    CASE
      WHEN pg_temp.lt_uniform('dense:' || a.seq::text) < a.dense_cluster_ratio THEN
        (CASE WHEN a.profile = 'local-dense' THEN a.local_center_lng ELSE z.base_lng END)
        + ((pg_temp.lt_pick_idx('slotlng:' || a.seq::text, 15) - 7)::numeric * a.dense_grid_step)
        + pg_temp.lt_centered('jitlng:' || a.seq::text, a.dense_jitter)
      WHEN a.zone_bucket = 'HOTSPOT' THEN
        (CASE WHEN a.profile = 'local-dense' THEN a.local_center_lng ELSE z.base_lng END)
        + pg_temp.lt_centered('lng:' || a.seq::text, a.hotspot_lng_spread)
      WHEN a.zone_bucket = 'MID' THEN
        (CASE WHEN a.profile = 'local-dense' THEN a.local_center_lng ELSE z.base_lng END)
        + pg_temp.lt_centered('lng:' || a.seq::text, a.mid_lng_spread)
      ELSE
        (CASE WHEN a.profile = 'local-dense' THEN a.local_center_lng ELSE z.base_lng END)
        + pg_temp.lt_centered('lng:' || a.seq::text, a.edge_lng_spread)
    END AS lng,
    CASE a.tier
      WHEN 'LONG_TAIL' THEN pg_temp.lt_rand_int('fc:' || a.seq::text, 1, 2, 1.00)
      WHEN 'MID_POPULAR' THEN pg_temp.lt_rand_int('fc:' || a.seq::text, 2, 4, 0.85)
      WHEN 'HOTSPOT_POPULAR' THEN pg_temp.lt_rand_int('fc:' || a.seq::text, 3, 5, 0.75)
      ELSE pg_temp.lt_rand_int('fc:' || a.seq::text, 4, 6, 0.65)
    END AS food_category_count,
    CASE a.tier
      WHEN 'LONG_TAIL' THEN pg_temp.lt_rand_int('mc:' || a.seq::text, 1, 2, 1.00)
      WHEN 'MID_POPULAR' THEN pg_temp.lt_rand_int('mc:' || a.seq::text, 2, 4, 0.90)
      WHEN 'HOTSPOT_POPULAR' THEN pg_temp.lt_rand_int('mc:' || a.seq::text, 3, 5, 0.85)
      ELSE pg_temp.lt_rand_int('mc:' || a.seq::text, 4, 6, 0.80)
    END AS menu_category_count,
    CASE a.tier
      WHEN 'LONG_TAIL' THEN pg_temp.lt_rand_int('menu:' || a.seq::text, 4, 12, 0.90)
      WHEN 'MID_POPULAR' THEN pg_temp.lt_rand_int('menu:' || a.seq::text, 10, 25, 0.82)
      WHEN 'HOTSPOT_POPULAR' THEN pg_temp.lt_rand_int('menu:' || a.seq::text, 20, 50, 0.78)
      ELSE pg_temp.lt_rand_int('menu:' || a.seq::text, 40, 120, 0.72)
    END AS menu_count,
    CASE a.tier
      WHEN 'LONG_TAIL' THEN pg_temp.lt_rand_int('img:' || a.seq::text, 0, 2, 1.10)
      WHEN 'MID_POPULAR' THEN pg_temp.lt_rand_int('img:' || a.seq::text, 2, 4, 0.80)
      WHEN 'HOTSPOT_POPULAR' THEN pg_temp.lt_rand_int('img:' || a.seq::text, 3, 6, 0.75)
      ELSE pg_temp.lt_rand_int('img:' || a.seq::text, 5, 10, 0.70)
    END AS image_count,
    CASE a.tier
      WHEN 'LONG_TAIL' THEN pg_temp.lt_rand_int('review:' || a.seq::text, 0, 8, 0.95)
      WHEN 'MID_POPULAR' THEN pg_temp.lt_rand_int('review:' || a.seq::text, 10, 40, 0.35)
      WHEN 'HOTSPOT_POPULAR' THEN pg_temp.lt_rand_int('review:' || a.seq::text, 50, 200, 0.22)
      ELSE pg_temp.lt_rand_int('review:' || a.seq::text, 300, 1500, 0.08)
    END AS review_count,
    CASE a.tier
      WHEN 'LONG_TAIL' THEN pg_temp.lt_rand_int('mf:' || a.seq::text, 0, 5, 1.05)
      WHEN 'MID_POPULAR' THEN pg_temp.lt_rand_int('mf:' || a.seq::text, 5, 20, 0.55)
      WHEN 'HOTSPOT_POPULAR' THEN pg_temp.lt_rand_int('mf:' || a.seq::text, 20, 80, 0.45)
      ELSE pg_temp.lt_rand_int('mf:' || a.seq::text, 100, 500, 0.35)
    END AS member_favorite_count,
    CASE a.tier
      WHEN 'LONG_TAIL' THEN pg_temp.lt_rand_int('sf:' || a.seq::text, 0, 2, 1.05)
      WHEN 'MID_POPULAR' THEN pg_temp.lt_rand_int('sf:' || a.seq::text, 2, 8, 0.60)
      WHEN 'HOTSPOT_POPULAR' THEN pg_temp.lt_rand_int('sf:' || a.seq::text, 5, 20, 0.45)
      ELSE pg_temp.lt_rand_int('sf:' || a.seq::text, 20, 80, 0.35)
    END AS subgroup_favorite_count
  FROM assigned a
  JOIN zone_center z ON z.center_key = a.center_key
)
SELECT
  c.*,
  format(
    '%s %s %s %s',
    (ARRAY['판교', '강남', '잠실', '역삼', '정자', '서현', '문정', '성수', '광교', '죽전', '미사', '다산'])[1 + pg_temp.lt_pick_idx('area:' || c.seq::text, 12)],
    (ARRAY['백반', '스시', '카페', '버거', '국밥', '파스타', '중식', '분식', '고기집', '샐러드'])[1 + pg_temp.lt_pick_idx('cuisine:' || c.seq::text, 10)],
    (ARRAY['하우스', '키친', '다이닝', '라운지', '테이블', '스토어'])[1 + pg_temp.lt_pick_idx('concept:' || c.seq::text, 6)],
    lpad(c.seq::text, 4, '0')
  ) AS restaurant_name,
  format(
    '010-%04s-%04s',
    lpad((1000 + (c.seq % 9000))::text, 4, '0'),
    lpad((2000 + ((c.seq * 7) % 8000))::text, 4, '0')
  ) AS phone_number,
  format(
    '%s %s %s %s번길 %s',
    c.sido,
    c.sigungu,
    c.eupmyeondong,
    1 + pg_temp.lt_pick_idx('street:' || c.seq::text, 80),
    1 + pg_temp.lt_pick_idx('building:' || c.seq::text, 220)
  ) AS full_address
FROM computed c;

INSERT INTO restaurant (
  id, name, phone_number, full_address, location, deleted_at, created_at, updated_at
)
SELECT
  restaurant_id,
  restaurant_name,
  phone_number,
  full_address,
  ST_GeomFromText(format('POINT(%s %s)', lng, lat), 4326),
  NULL,
  now(),
  now()
FROM lt_restaurant_seed
ON CONFLICT DO NOTHING;

INSERT INTO restaurant_address (
  id, restaurant_id, sido, sigungu, eupmyeondong, postal_code, created_at, updated_at
)
SELECT
  restaurant_id * 10 + 1,
  restaurant_id,
  sido,
  sigungu,
  eupmyeondong,
  lpad((10000 + pg_temp.lt_pick_idx('postal:' || seq::text, 89999))::text, 5, '0'),
  now(),
  now()
FROM lt_restaurant_seed
ON CONFLICT DO NOTHING;

INSERT INTO restaurant_food_category (id, restaurant_id, food_category_id)
SELECT
  rs.restaurant_id * 10 + fc_idx,
  rs.restaurant_id,
  fcp.id
FROM lt_restaurant_seed rs
CROSS JOIN LATERAL generate_series(1, rs.food_category_count) AS fc_idx
JOIN lt_food_category_pool fcp
  ON fcp.idx = (
    pg_temp.lt_pick_idx('fc-offset:' || rs.restaurant_id::text, (SELECT count(*) FROM lt_food_category_pool))
    + fc_idx - 1
  ) % (SELECT count(*) FROM lt_food_category_pool)
ON CONFLICT DO NOTHING;

INSERT INTO restaurant_weekly_schedule (
  id, restaurant_id, day_of_week, open_time, close_time, is_closed,
  effective_from, effective_to, created_at, updated_at
)
SELECT
  rs.restaurant_id * 10 + d.day_of_week + 100,
  rs.restaurant_id,
  d.day_of_week,
  (
  CASE
    WHEN rs.tier = 'LONG_TAIL' AND d.day_of_week = 7 THEN NULL
    WHEN rs.tier = 'MID_POPULAR' AND d.day_of_week = 7 THEN '10:30'
    WHEN rs.tier = 'HOTSPOT_POPULAR' THEN CASE WHEN d.day_of_week IN (6, 7) THEN '10:00' ELSE '10:30' END
    WHEN rs.tier = 'TOP_DENSE' THEN CASE WHEN d.day_of_week IN (6, 7) THEN '09:30' ELSE '10:00' END
    ELSE CASE WHEN d.day_of_week = 6 THEN '11:00' ELSE '11:30' END
  END
  )::time,
  (
  CASE
    WHEN rs.tier = 'LONG_TAIL' AND d.day_of_week = 7 THEN NULL
    WHEN rs.tier = 'MID_POPULAR' THEN CASE WHEN d.day_of_week IN (5, 6) THEN '22:00' ELSE '21:30' END
    WHEN rs.tier = 'HOTSPOT_POPULAR' THEN CASE WHEN d.day_of_week IN (5, 6) THEN '23:30' ELSE '22:30' END
    WHEN rs.tier = 'TOP_DENSE' THEN CASE WHEN d.day_of_week IN (5, 6) THEN '01:00' ELSE '23:30' END
    ELSE CASE WHEN d.day_of_week = 6 THEN '20:30' ELSE '21:00' END
  END
  )::time,
  CASE
    WHEN rs.tier = 'LONG_TAIL' AND d.day_of_week = 7 THEN true
    ELSE false
  END,
  NULL,
  NULL,
  now(),
  now()
FROM lt_restaurant_seed rs
CROSS JOIN (SELECT generate_series(1, 7) AS day_of_week) d
ON CONFLICT DO NOTHING;

CREATE TEMP TABLE lt_menu_category_seed AS
SELECT
  rs.restaurant_id,
  cat_idx AS category_ordinal,
  rs.restaurant_id * 10 + 20 + cat_idx AS category_id,
  (ARRAY['메인', '세트', '사이드', '음료', '디저트', '시즌한정'])[1 + ((cat_idx - 1) % 6)] AS category_name
FROM lt_restaurant_seed rs
CROSS JOIN LATERAL generate_series(1, rs.menu_category_count) AS cat_idx;

INSERT INTO menu_category (
  id, restaurant_id, name, display_order, created_at, updated_at
)
SELECT
  category_id,
  restaurant_id,
  category_name,
  category_ordinal - 1,
  now(),
  now()
FROM lt_menu_category_seed
ON CONFLICT DO NOTHING;

INSERT INTO menu (
  id, category_id, name, description, price, image_url, is_recommended, display_order, created_at, updated_at
)
SELECT
  rs.restaurant_id * 1000 + 200 + menu_idx,
  mc.category_id,
  format(
    '%s %s %s',
    (ARRAY['시그니처', '클래식', '프리미엄', '데일리', '스페셜', '베스트'])[1 + pg_temp.lt_pick_idx('menu-prefix:' || rs.restaurant_id::text || ':' || menu_idx::text, 6)],
    (ARRAY['정식', '플래터', '초밥', '파스타', '버거', '라멘', '덮밥', '샐러드', '커피', '디저트'])[1 + pg_temp.lt_pick_idx('menu-core:' || rs.restaurant_id::text || ':' || menu_idx::text, 10)],
    lpad(menu_idx::text, 2, '0')
  ),
  format(
    '%s 매장의 %s 카테고리 %s번 메뉴',
    rs.restaurant_name,
    mc.category_name,
    menu_idx
  ),
  CASE rs.tier
    WHEN 'LONG_TAIL' THEN 6500 + pg_temp.lt_pick_idx('price:' || rs.restaurant_id::text || ':' || menu_idx::text, 12) * 500
    WHEN 'MID_POPULAR' THEN 8500 + pg_temp.lt_pick_idx('price:' || rs.restaurant_id::text || ':' || menu_idx::text, 18) * 700
    WHEN 'HOTSPOT_POPULAR' THEN 11000 + pg_temp.lt_pick_idx('price:' || rs.restaurant_id::text || ':' || menu_idx::text, 24) * 900
    ELSE 16000 + pg_temp.lt_pick_idx('price:' || rs.restaurant_id::text || ':' || menu_idx::text, 45) * 1200
  END,
  NULL,
  CASE
    WHEN menu_idx <= GREATEST(1, ceil(rs.menu_count * 0.15)::integer) THEN true
    ELSE false
  END,
  menu_idx - 1,
  now(),
  now()
FROM lt_restaurant_seed rs
CROSS JOIN LATERAL generate_series(1, rs.menu_count) AS menu_idx
JOIN lt_menu_category_seed mc
  ON mc.restaurant_id = rs.restaurant_id
 AND mc.category_ordinal = 1 + ((menu_idx - 1) % rs.menu_category_count)
ON CONFLICT DO NOTHING;

INSERT INTO image (
  id, file_name, file_size, file_type, storage_key, file_uuid, status, purpose, deleted_at, created_at, updated_at
)
SELECT
  rs.restaurant_id * 100 + img_idx,
  format('restaurant-%s-%s.jpg', rs.restaurant_id, img_idx),
  120000 + img_idx * 137 + (rs.seq % 5000),
  'image/jpeg',
  format('seed/load-test/restaurants/%s-%s.jpg', rs.restaurant_id, img_idx),
  format('00000000-0000-0000-0000-%s', lpad((rs.restaurant_id * 100 + img_idx)::text, 12, '0'))::uuid,
  'ACTIVE',
  'RESTAURANT_IMAGE',
  NULL,
  now(),
  now()
FROM lt_restaurant_seed rs
CROSS JOIN LATERAL generate_series(1, rs.image_count) AS img_idx
ON CONFLICT DO NOTHING;

INSERT INTO domain_image (
  id, domain_type, domain_id, image_id, sort_order, created_at
)
SELECT
  rs.restaurant_id * 100 + img_idx,
  'RESTAURANT',
  rs.restaurant_id,
  rs.restaurant_id * 100 + img_idx,
  img_idx - 1,
  now()
FROM lt_restaurant_seed rs
CROSS JOIN LATERAL generate_series(1, rs.image_count) AS img_idx
ON CONFLICT DO NOTHING;

CREATE TEMP TABLE lt_review_seed AS
SELECT
  {review_start_id} + row_number() OVER (ORDER BY rs.restaurant_id, review_idx) - 1 AS review_id,
  rs.restaurant_id,
  rs.tier,
  rs.zone_bucket,
  rs.restaurant_name,
  review_idx,
  mp.id AS member_id,
  gp.id AS group_id,
    CASE
      WHEN (SELECT count(*) FROM lt_subgroup_pool) = 0 THEN NULL
      WHEN rs.tier = 'LONG_TAIL' AND review_idx % 3 = 0 THEN NULL
      WHEN review_idx % 7 = 0 THEN NULL
      ELSE sgp.id
    END AS subgroup_id,
  CASE rs.tier
    WHEN 'LONG_TAIL' THEN (pg_temp.lt_uniform('rec:' || rs.restaurant_id::text || ':' || review_idx::text) < 0.62)
    WHEN 'MID_POPULAR' THEN (pg_temp.lt_uniform('rec:' || rs.restaurant_id::text || ':' || review_idx::text) < 0.74)
    WHEN 'HOTSPOT_POPULAR' THEN (pg_temp.lt_uniform('rec:' || rs.restaurant_id::text || ':' || review_idx::text) < 0.82)
    ELSE (pg_temp.lt_uniform('rec:' || rs.restaurant_id::text || ':' || review_idx::text) < 0.88)
  END AS is_recommended,
  (
    now()
    - make_interval(days => pg_temp.lt_pick_idx('review-day:' || rs.restaurant_id::text || ':' || review_idx::text, 540))
    - make_interval(mins => pg_temp.lt_pick_idx('review-minute:' || rs.restaurant_id::text || ':' || review_idx::text, 1440))
  ) AS created_at,
  CASE
    WHEN rs.tier = 'LONG_TAIL' THEN 1 + (review_idx % 2)
    WHEN rs.tier = 'MID_POPULAR' THEN 2
    ELSE 2 + (review_idx % 2)
  END AS keyword_count,
  format(
    '%s. %s. %s.',
    (ARRAY['재방문 의사 있습니다', '근처에서 무난하게 선택하기 좋습니다', '대기만 감수하면 만족도가 높습니다', '메뉴 구성이 안정적입니다', '시간대별 편차는 있지만 전반적으로 괜찮습니다'])[1 + pg_temp.lt_pick_idx('review-a:' || rs.restaurant_id::text || ':' || review_idx::text, 5)],
    (ARRAY['점심 회전이 빠릅니다', '모임으로 이용하기 좋습니다', '혼밥도 무난합니다', '대표 메뉴 완성도가 좋습니다', '가격 대비 만족도가 준수합니다'])[1 + pg_temp.lt_pick_idx('review-b:' || rs.restaurant_id::text || ':' || review_idx::text, 5)],
    (ARRAY['재료 신선도가 괜찮았습니다', '좌석 간격이 넉넉합니다', '서비스가 빠른 편입니다', '주말에는 붐비는 편입니다', '메뉴 선택지가 다양합니다'])[1 + pg_temp.lt_pick_idx('review-c:' || rs.restaurant_id::text || ':' || review_idx::text, 5)]
  ) AS content
FROM lt_restaurant_seed rs
CROSS JOIN LATERAL generate_series(1, rs.review_count) AS review_idx
JOIN lt_member_pool mp
  ON mp.idx = (
    pg_temp.lt_pick_idx('member-base:' || rs.restaurant_id::text, (SELECT count(*) FROM lt_member_pool))
    + review_idx - 1
  ) % (SELECT count(*) FROM lt_member_pool)
JOIN lt_group_pool gp
  ON gp.idx = (
    pg_temp.lt_pick_idx('group-base:' || rs.restaurant_id::text, (SELECT count(*) FROM lt_group_pool))
    + review_idx - 1
  ) % (SELECT count(*) FROM lt_group_pool)
JOIN lt_subgroup_pool sgp
  ON sgp.idx = (
    pg_temp.lt_pick_idx('subgroup-base:' || rs.restaurant_id::text, (SELECT count(*) FROM lt_subgroup_pool))
    + review_idx - 1
  ) % (SELECT count(*) FROM lt_subgroup_pool);

INSERT INTO review (
  id, restaurant_id, member_id, group_id, subgroup_id, content, is_recommended, deleted_at, created_at, updated_at
)
SELECT
  review_id,
  restaurant_id,
  member_id,
  group_id,
  subgroup_id,
  content,
  is_recommended,
  NULL,
  created_at,
  created_at
FROM lt_review_seed
ON CONFLICT DO NOTHING;

CREATE TEMP TABLE lt_review_keyword_seed AS
SELECT
  {review_keyword_start_id} + row_number() OVER (ORDER BY r.review_id, kw_idx) - 1,
  r.review_id,
  kp.id
FROM lt_review_seed r
CROSS JOIN LATERAL generate_series(1, r.keyword_count) AS kw_idx
JOIN lt_keyword_pool kp
  ON kp.idx = (
    pg_temp.lt_pick_idx('keyword-base:' || r.review_id::text, (SELECT count(*) FROM lt_keyword_pool))
    + kw_idx - 1
  ) % (SELECT count(*) FROM lt_keyword_pool)
;

INSERT INTO review_keyword (id, review_id, keyword_id)
SELECT * FROM lt_review_keyword_seed
ON CONFLICT DO NOTHING;

CREATE TEMP TABLE lt_member_favorite_seed AS
SELECT
  {member_favorite_start_id} + row_number() OVER (ORDER BY rs.restaurant_id, fav_idx) - 1 AS favorite_id,
  rs.restaurant_id,
  mp.id AS member_id,
  now()
    - make_interval(days => pg_temp.lt_pick_idx('fav-day:' || rs.restaurant_id::text || ':' || fav_idx::text, 365))
    - make_interval(mins => pg_temp.lt_pick_idx('fav-minute:' || rs.restaurant_id::text || ':' || fav_idx::text, 1440)) AS created_at
FROM lt_restaurant_seed rs
CROSS JOIN LATERAL generate_series(
  1,
  LEAST(rs.member_favorite_count, (SELECT count(*) FROM lt_member_pool))
) AS fav_idx
JOIN lt_member_pool mp
  ON mp.idx = (
    pg_temp.lt_pick_idx('fav-member-base:' || rs.restaurant_id::text, (SELECT count(*) FROM lt_member_pool))
    + fav_idx - 1
  ) % (SELECT count(*) FROM lt_member_pool);

INSERT INTO member_favorite_restaurant (
  id, member_id, restaurant_id, deleted_at, created_at
)
SELECT favorite_id, member_id, restaurant_id, NULL, created_at
FROM lt_member_favorite_seed
ON CONFLICT DO NOTHING;

CREATE TEMP TABLE lt_subgroup_favorite_seed AS
SELECT
  {subgroup_favorite_start_id} + row_number() OVER (ORDER BY rs.restaurant_id, fav_idx) - 1 AS favorite_id,
  rs.restaurant_id,
  mp.id AS member_id,
  sgp.id AS subgroup_id,
  now()
    - make_interval(days => pg_temp.lt_pick_idx('sgfav-day:' || rs.restaurant_id::text || ':' || fav_idx::text, 365))
    - make_interval(mins => pg_temp.lt_pick_idx('sgfav-minute:' || rs.restaurant_id::text || ':' || fav_idx::text, 1440)) AS created_at
FROM lt_restaurant_seed rs
CROSS JOIN LATERAL generate_series(
  1,
  LEAST(rs.subgroup_favorite_count, (SELECT count(*) FROM lt_subgroup_pool))
) AS fav_idx
JOIN lt_member_pool mp
  ON mp.idx = (
    pg_temp.lt_pick_idx('sgfav-member-base:' || rs.restaurant_id::text, (SELECT count(*) FROM lt_member_pool))
    + fav_idx - 1
  ) % (SELECT count(*) FROM lt_member_pool)
JOIN lt_subgroup_pool sgp
  ON sgp.idx = (
    pg_temp.lt_pick_idx('sgfav-subgroup-base:' || rs.restaurant_id::text, (SELECT count(*) FROM lt_subgroup_pool))
    + fav_idx - 1
  ) % (SELECT count(*) FROM lt_subgroup_pool);

INSERT INTO subgroup_favorite_restaurant (
  id, member_id, restaurant_id, subgroup_id, deleted_at, created_at
)
SELECT favorite_id, member_id, restaurant_id, subgroup_id, NULL, created_at
FROM lt_subgroup_favorite_seed
ON CONFLICT DO NOTHING;

INSERT INTO restaurant_review_summary (
  id, restaurant_id, vector_epoch, model_version, summary_json, analyzed_at
)
SELECT
  restaurant_id + {summary_id_offset},
  restaurant_id,
  0,
  'load-test-v1',
  jsonb_build_object(
    'overall_summary',
    format(
      '%s 권역의 %s 매장입니다. 리뷰 %s건, 메뉴 %s개, 즐겨찾기 %s건 규모입니다.',
      zone_bucket,
      tier,
      review_count,
      menu_count,
      member_favorite_count + subgroup_favorite_count
    ),
    'categories',
    jsonb_build_object(
      'service', format('추천 비중이 %s%% 수준으로 형성된 매장입니다.', CASE tier WHEN 'LONG_TAIL' THEN 62 WHEN 'MID_POPULAR' THEN 74 WHEN 'HOTSPOT_POPULAR' THEN 82 ELSE 88 END),
      'price', format('메뉴 수 %s개 기반으로 가격 편차가 큰 편입니다.', menu_count),
      'food', format('음식 카테고리 %s개가 연결되어 있습니다.', food_category_count)
    )
  ),
  now()
FROM lt_restaurant_seed
ON CONFLICT DO NOTHING;

INSERT INTO restaurant_review_sentiment (
  id, restaurant_id, vector_epoch, model_version,
  positive_count, negative_count, neutral_count,
  positive_percent, negative_percent, neutral_percent, analyzed_at
)
SELECT
  restaurant_id + {sentiment_id_offset},
  restaurant_id,
  0,
  'load-test-v1',
  positive_count,
  negative_count,
  review_count - positive_count - negative_count,
  round(positive_count * 100.0 / GREATEST(review_count, 1), 2),
  round(negative_count * 100.0 / GREATEST(review_count, 1), 2),
  round((review_count - positive_count - negative_count) * 100.0 / GREATEST(review_count, 1), 2),
  now()
FROM (
  SELECT
    rs.*,
    floor(review_count * CASE tier WHEN 'LONG_TAIL' THEN 0.62 WHEN 'MID_POPULAR' THEN 0.71 WHEN 'HOTSPOT_POPULAR' THEN 0.79 ELSE 0.84 END)::integer AS positive_count,
    floor(review_count * CASE tier WHEN 'LONG_TAIL' THEN 0.14 WHEN 'MID_POPULAR' THEN 0.11 WHEN 'HOTSPOT_POPULAR' THEN 0.08 ELSE 0.06 END)::integer AS negative_count
  FROM lt_restaurant_seed rs
) s
ON CONFLICT DO NOTHING;

INSERT INTO restaurant_comparison (
  id, restaurant_id, model_version, comparison_json, analyzed_at
)
SELECT
  restaurant_id + {comparison_id_offset},
  restaurant_id,
  'load-test-v1',
  jsonb_build_object(
    'category_lift',
    jsonb_build_object(
      'service', round((0.02 + pg_temp.lt_uniform('cmp-service:' || restaurant_id::text) * 0.18)::numeric, 3),
      'price', round((-0.08 + pg_temp.lt_uniform('cmp-price:' || restaurant_id::text) * 0.24)::numeric, 3),
      'food', round((0.04 + pg_temp.lt_uniform('cmp-food:' || restaurant_id::text) * 0.20)::numeric, 3)
    ),
    'comparison_display',
    jsonb_build_array(
      format('%s 권역 내 %s 분포를 반영한 비교 결과입니다.', zone_bucket, tier)
    ),
    'total_candidates', 30 + pg_temp.lt_pick_idx('cmp-total:' || restaurant_id::text, 90),
    'validated_count', 15 + pg_temp.lt_pick_idx('cmp-valid:' || restaurant_id::text, 45)
  ),
  now()
FROM lt_restaurant_seed
ON CONFLICT DO NOTHING;

SELECT 'restaurants' AS entity, count(*)::bigint AS generated_rows FROM lt_restaurant_seed
UNION ALL
SELECT 'menu_categories', count(*)::bigint FROM lt_menu_category_seed
UNION ALL
SELECT 'menus', sum(menu_count)::bigint FROM lt_restaurant_seed
UNION ALL
SELECT 'images', sum(image_count)::bigint FROM lt_restaurant_seed
UNION ALL
SELECT 'reviews', count(*)::bigint FROM lt_review_seed
UNION ALL
SELECT 'review_keywords', count(*)::bigint FROM lt_review_keyword_seed
UNION ALL
SELECT 'member_favorites', count(*)::bigint FROM lt_member_favorite_seed
UNION ALL
SELECT 'subgroup_favorites', count(*)::bigint FROM lt_subgroup_favorite_seed
ORDER BY entity;

COMMIT;
"""


def lt_hash60(key: str) -> int:
    return int(hashlib.md5(key.encode()).hexdigest()[:15], 16)


def lt_uniform(key: str) -> float:
    return (lt_hash60(key) % 1_000_000) / 999_999.0


def lt_rand_int(key: str, min_value: int, max_value: int, skew: float = 1.0) -> int:
    span = max_value - min_value + 1
    return min(max_value, min_value + math.floor((lt_uniform(key) ** skew) * span))


def estimate_targets(restaurant_count: int) -> tuple[int, int]:
    menus = 0
    reviews = 0

    for seq in range(1, restaurant_count + 1):
        if seq <= math.floor(restaurant_count * 0.70):
            tier = "LONG_TAIL"
        elif seq <= math.floor(restaurant_count * 0.90):
            tier = "MID_POPULAR"
        elif seq <= math.floor(restaurant_count * 0.98):
            tier = "HOTSPOT_POPULAR"
        else:
            tier = "TOP_DENSE"

        if tier == "LONG_TAIL":
            menus += lt_rand_int(f"menu:{seq}", 4, 12, 0.90)
            reviews += lt_rand_int(f"review:{seq}", 0, 8, 0.95)
        elif tier == "MID_POPULAR":
            menus += lt_rand_int(f"menu:{seq}", 10, 25, 0.82)
            reviews += lt_rand_int(f"review:{seq}", 10, 40, 0.35)
        elif tier == "HOTSPOT_POPULAR":
            menus += lt_rand_int(f"menu:{seq}", 20, 50, 0.78)
            reviews += lt_rand_int(f"review:{seq}", 50, 200, 0.22)
        else:
            menus += lt_rand_int(f"menu:{seq}", 40, 120, 0.72)
            reviews += lt_rand_int(f"review:{seq}", 300, 1500, 0.08)

    return menus, reviews


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate deterministic load-test seed SQL")
    parser.add_argument("--restaurant-count", type=int, default=10000, help="number of restaurants to generate")
    parser.add_argument("--restaurant-start-id", type=int, default=500000, help="restaurant id base")
    parser.add_argument(
        "--profile",
        choices=["citywide", "local-dense"],
        default="citywide",
        help="citywide 분산 또는 단일 좌표 중심 밀집 프로필",
    )
    parser.add_argument(
        "--out",
        default="restaurant_load_test_seed.sql",
        help="output SQL file path",
    )
    parser.add_argument("--min-members", type=int, default=1, help="minimum required members")
    parser.add_argument("--min-groups", type=int, default=1, help="minimum required groups")
    parser.add_argument("--min-subgroups", type=int, default=1, help="minimum required subgroups")
    parser.add_argument("--min-keywords", type=int, default=1, help="minimum required keywords")
    parser.add_argument("--min-food-categories", type=int, default=6, help="minimum required food categories")
    parser.add_argument("--member-target-count", type=int, default=1200, help="auto-fill member count target")
    parser.add_argument("--member-start-id", type=int, default=900000, help="load-test member id base")
    parser.add_argument("--local-center-lat", type=float, default=37.402052, help="local-dense 중심 위도")
    parser.add_argument("--local-center-lng", type=float, default=127.107058, help="local-dense 중심 경도")
    parser.add_argument("--local-center-sido", default="경기도", help="local-dense 주소 시도")
    parser.add_argument("--local-center-sigungu", default="성남시 분당구", help="local-dense 주소 시군구")
    parser.add_argument("--local-center-eupmyeondong", default="삼평동", help="local-dense 주소 읍면동")
    parser.add_argument("--dense-cluster-ratio", type=float, default=0.35, help="격자형 초밀집 비율")
    parser.add_argument("--dense-grid-step", type=float, default=0.00018, help="밀집 격자 간격")
    parser.add_argument("--dense-jitter", type=float, default=0.00001, help="밀집 좌표 미세 흔들림")
    parser.add_argument("--hotspot-lat-spread", type=float, default=0.01050, help="HOTSPOT 위도 분산")
    parser.add_argument("--hotspot-lng-spread", type=float, default=0.01250, help="HOTSPOT 경도 분산")
    parser.add_argument("--mid-lat-spread", type=float, default=0.02400, help="MID 위도 분산")
    parser.add_argument("--mid-lng-spread", type=float, default=0.02800, help="MID 경도 분산")
    parser.add_argument("--edge-lat-spread", type=float, default=0.06800, help="EDGE 위도 분산")
    parser.add_argument("--edge-lng-spread", type=float, default=0.08200, help="EDGE 경도 분산")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.profile == "local-dense":
        args.dense_cluster_ratio = 0.92 if args.dense_cluster_ratio == 0.35 else args.dense_cluster_ratio
        args.dense_grid_step = 0.00008 if args.dense_grid_step == 0.00018 else args.dense_grid_step
        args.dense_jitter = 0.000005 if args.dense_jitter == 0.00001 else args.dense_jitter
        args.hotspot_lat_spread = 0.0022 if args.hotspot_lat_spread == 0.01050 else args.hotspot_lat_spread
        args.hotspot_lng_spread = 0.0028 if args.hotspot_lng_spread == 0.01250 else args.hotspot_lng_spread
        args.mid_lat_spread = 0.0045 if args.mid_lat_spread == 0.02400 else args.mid_lat_spread
        args.mid_lng_spread = 0.0055 if args.mid_lng_spread == 0.02800 else args.mid_lng_spread
        args.edge_lat_spread = 0.0075 if args.edge_lat_spread == 0.06800 else args.edge_lat_spread
        args.edge_lng_spread = 0.0090 if args.edge_lng_spread == 0.08200 else args.edge_lng_spread

    target_menus, target_reviews = estimate_targets(args.restaurant_count)
    sql = SQL_TEMPLATE.format(
        restaurant_count=args.restaurant_count,
        restaurant_start_id=args.restaurant_start_id,
        target_menus=target_menus,
        target_reviews=target_reviews,
        profile=args.profile,
        min_members=args.min_members,
        min_groups=args.min_groups,
        min_subgroups=args.min_subgroups,
        min_keywords=args.min_keywords,
        min_food_categories=args.min_food_categories,
        member_target_count=args.member_target_count,
        member_start_id=args.member_start_id,
        group_start_id=910_000,
        group_member_start_id=920_000,
        subgroup_start_id=930_000,
        subgroup_member_start_id=940_000,
        keyword_start_id=950_000,
        food_category_start_id=960_000,
        local_center_lat=args.local_center_lat,
        local_center_lng=args.local_center_lng,
        local_center_sido=args.local_center_sido,
        local_center_sigungu=args.local_center_sigungu,
        local_center_eupmyeondong=args.local_center_eupmyeondong,
        dense_cluster_ratio=args.dense_cluster_ratio,
        dense_grid_step=args.dense_grid_step,
        dense_jitter=args.dense_jitter,
        hotspot_lat_spread=args.hotspot_lat_spread,
        hotspot_lng_spread=args.hotspot_lng_spread,
        mid_lat_spread=args.mid_lat_spread,
        mid_lng_spread=args.mid_lng_spread,
        edge_lat_spread=args.edge_lat_spread,
        edge_lng_spread=args.edge_lng_spread,
        review_start_id=8_500_000,
        review_keyword_start_id=9_500_000,
        member_favorite_start_id=10_500_000,
        subgroup_favorite_start_id=11_500_000,
        summary_id_offset=1_200_000,
        sentiment_id_offset=1_300_000,
        comparison_id_offset=1_400_000,
    )

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(sql, encoding="utf-8")
    print(f"written: {out_path}")


if __name__ == "__main__":
    main()
