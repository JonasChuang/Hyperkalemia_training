USE mimiciv;

-- 0) 目標與黑名單設定
-- 超過此值視為高血鉀
SET @K_CUTOFF := 5.5;

-- 1) 住院 0–24h / 24–48h 時窗
DROP TEMPORARY TABLE IF EXISTS win48;
CREATE TEMPORARY TABLE win48 AS
SELECT a.subject_id, a.hadm_id,
       a.admittime                           AS t0,
       TIMESTAMPADD(HOUR, 24, a.admittime)  AS t24,
       TIMESTAMPADD(HOUR, 48, a.admittime)  AS t48
FROM admissions a;

-- 2) 取 0–24h 生命徵象（不含 K）
DROP TEMPORARY TABLE IF EXISTS vitals24;
CREATE TEMPORARY TABLE vitals24 AS
SELECT w.hadm_id, UPPER(di.label) AS label,
       AVG(ce.valuenum) AS mean_v, MIN(ce.valuenum) AS min_v, MAX(ce.valuenum) AS max_v
FROM chartevents ce
JOIN d_items di ON di.itemid = ce.itemid
JOIN win48 w    ON w.hadm_id = ce.hadm_id
WHERE ce.valuenum IS NOT NULL
  AND ce.charttime >= w.t0 AND ce.charttime < w.t24
  AND UPPER(di.label) IN (
    'HEART RATE','RESPIRATORY RATE','SPO2','TEMPERATURE CELSIUS',
    'NON INVASIVE BLOOD PRESSURE SYSTOLIC',
    'NON INVASIVE BLOOD PRESSURE DIASTOLIC',
    'MEAN ARTERIAL PRESSURE (NIBP)'
  )
GROUP BY w.hadm_id, UPPER(di.label);

-- 3) 取 0–24h 化驗（不含 Potassium，避免洩漏）
DROP TEMPORARY TABLE IF EXISTS labs24;
CREATE TEMPORARY TABLE labs24 AS
SELECT w.hadm_id, UPPER(dl.label) AS label,
       AVG(le.valuenum) AS mean_v, MIN(le.valuenum) AS min_v, MAX(le.valuenum) AS max_v
FROM labevents le
JOIN d_labitems dl ON dl.itemid = le.itemid
JOIN win48 w       ON w.hadm_id = le.hadm_id
WHERE le.valuenum IS NOT NULL
  AND le.charttime >= w.t0 AND le.charttime < w.t24
  AND UPPER(dl.label) IN ('SODIUM','BICARBONATE','CHLORIDE','CREATININE','UREA NITROGEN','GLUCOSE')
GROUP BY w.hadm_id, UPPER(dl.label);

-- 4) 將 0–24h 特徵 pivot
DROP TEMPORARY TABLE IF EXISTS feat24;
CREATE TEMPORARY TABLE feat24 AS
SELECT w.hadm_id,
  -- Vitals
  MAX(CASE WHEN v.label='HEART RATE' THEN v.mean_v END) AS hr_mean,
  MAX(CASE WHEN v.label='RESPIRATORY RATE' THEN v.mean_v END) AS rr_mean,
  MAX(CASE WHEN v.label='SPO2' THEN v.min_v END) AS spo2_min,
  MAX(CASE WHEN v.label='TEMPERATURE CELSIUS' THEN v.max_v END) AS temp_max,
  MAX(CASE WHEN v.label='NON INVASIVE BLOOD PRESSURE SYSTOLIC' THEN v.min_v END) AS sbp_min,
  MAX(CASE WHEN v.label='NON INVASIVE BLOOD PRESSURE DIASTOLIC' THEN v.min_v END) AS dbp_min,
  MAX(CASE WHEN v.label='MEAN ARTERIAL PRESSURE (NIBP)' THEN v.min_v END) AS map_min,
  -- Labs (no K)
  MAX(CASE WHEN l.label='SODIUM' THEN l.mean_v END) AS sodium_mean,
  MAX(CASE WHEN l.label='BICARBONATE' THEN l.mean_v END) AS bicarb_mean,
  MAX(CASE WHEN l.label='CHLORIDE' THEN l.mean_v END) AS chloride_mean,
  MAX(CASE WHEN l.label='CREATININE' THEN l.max_v END) AS creatinine_max,
  MAX(CASE WHEN l.label='UREA NITROGEN' THEN l.max_v END) AS bun_max,
  MAX(CASE WHEN l.label='GLUCOSE' THEN l.max_v END) AS glucose_max
FROM win48 w
LEFT JOIN vitals24 v ON v.hadm_id = w.hadm_id
LEFT JOIN labs24   l ON l.hadm_id = w.hadm_id
GROUP BY w.hadm_id;

-- 5) 在 24–48h 內是否發生第一次高血鉀（label）
DROP TEMPORARY TABLE IF EXISTS k_24_48;
CREATE TEMPORARY TABLE k_24_48 AS
SELECT w.hadm_id,
       MIN(CASE WHEN le.valuenum >= @K_CUTOFF THEN le.charttime END) AS first_hk_time,
       MAX(CASE WHEN le.valuenum >= @K_CUTOFF THEN 1 ELSE 0 END)     AS has_hk_24_48
FROM win48 w
JOIN labevents le ON le.hadm_id = w.hadm_id
JOIN d_labitems dl ON dl.itemid = le.itemid
WHERE UPPER(dl.label) = 'POTASSIUM'
  AND le.charttime >= w.t24 AND le.charttime < w.t48
GROUP BY w.hadm_id;

-- 6) 最終訓練表：0–24h 特徵 + 24–48h 高血鉀標籤
DROP TABLE IF EXISTS hk_features_24to48;
CREATE TABLE hk_features_24to48 AS
SELECT
  a.subject_id, a.hadm_id, a.admittime,
  f.*,
  COALESCE(k.has_hk_24_48, 0) AS y_hk_24_48
FROM admissions a
JOIN feat24 f ON f.hadm_id = a.hadm_id
LEFT JOIN k_24_48 k ON k.hadm_id = a.hadm_id;

--(6) 轉檔用藥

DROP TABLE IF EXISTS z_k_drug;

CREATE  TABLE z_k_drug as

select a.hadm_id,a.starttime,
CASE WHEN a.drug LIKE '%lisinopril%' THEN 'RAASi'
    WHEN a.drug LIKE '%enalapril%' THEN 'RAASi'
    WHEN a.drug LIKE '%ramipril%' THEN 'RAASi'
    WHEN a.drug LIKE '%losartan%' THEN 'RAASi'
    WHEN a.drug LIKE '%irbesartan%' THEN 'RAASi'
    WHEN a.drug LIKE '%spironolactone%' THEN 'K_SPARING'
    WHEN a.drug LIKE '%eplerenone%' THEN 'K_SPARING'
    WHEN a.drug LIKE '%amiloride%' THEN 'K_SPARINGSi'
    WHEN a.drug LIKE '%triamterene%' THEN 'K_SPARING'
    WHEN a.drug LIKE '%heparin%' THEN 'HEPARIN'
    WHEN a.drug LIKE '%enoxaparin%' THEN 'HEPARIN'
    WHEN a.drug LIKE '%ibuprofen%' THEN 'NSAID'
    WHEN a.drug LIKE '%ketorolac%' THEN 'NSAID'
    WHEN a.drug LIKE '%furosemide%' THEN 'LOOP'
    WHEN a.drug LIKE '%bumetanide%' THEN 'LOOP'
    WHEN a.drug LIKE '%torsemide%' THEN 'LOOP'
    WHEN a.drug LIKE '%hydrochlorothiazide%' THEN 'THIAZIDE'
    WHEN a.drug LIKE '%chlorthalidone%' THEN 'THIAZIDE'
    end as drug,'1' as valuenum



 from prescriptions a
inner join z_k_adm b on(b.subject_id=a.subject_id and b.hadm_id=a.hadm_id)
where (LOWER(a.drug) like '%lisinopril%'
or LOWER(a.drug) like '%enalapril%'
or LOWER(a.drug) like '%ramipril%'
or LOWER(a.drug) like '%losartan%'
or LOWER(a.drug) like '%valsartan%'
or LOWER(a.drug) like '%irbesartan%'
or LOWER(a.drug) like '%spironolactone%'
or LOWER(a.drug) like '%eplerenone%'
or LOWER(a.drug) like '%amiloride%'
or LOWER(a.drug) like '%triamterene%'

or LOWER(a.drug) like '%heparin%'
or LOWER(a.drug) like '%enoxaparin%'
or LOWER(a.drug) like '%ibuprofen%'
or LOWER(a.drug) like '%ketorolac%'
or LOWER(a.drug) like '%furosemide%'
or LOWER(a.drug) like '%bumetanide%'
or LOWER(a.drug) like '%torsemide%'
or LOWER(a.drug) like '%hydrochlorothiazide%'
or LOWER(a.drug) like '%chlorthalidone%'
)
