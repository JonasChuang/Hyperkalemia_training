#MIMIC 資料庫轉檔

import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine, text

from scipy.stats import mannwhitneyu,chi2_contingency,ttest_ind, fisher_exact

#from main import ENG
ENG = create_engine("mysql+pymysql://test:test@10.2.163.201:3306/mimic3_1")# 連接設定


LAB_IN="""
  
                50852	, -- % Hemoglobin A1c
                50862	, -- Albumin
                51069	, -- Albumin, Urine
                50804	, -- Calculated Total CO2
                50806 ,-- Chloride, Whole Blood
                50902 ,-- Chloride
                50904	, -- Cholesterol, HDL
                50905	, -- Cholesterol, LDL, Calculated
                50907	, -- Cholesterol, Total
                50912	, -- Creatinine
                50808	, -- Free Calcium
                50813 , -- Lactate
                50893 , -- Calcium, Total 總鈣
                50931	, -- Glucose
                50960 , -- Magnesium 鎂離子
                50811	, -- Hemoglobin
                51222	, -- Hemoglobin
                51301 ,  -- White Blood Cells
                50818	, -- pCO2
                50820	, -- pH
                50970	, -- Phosphate
                50971	, -- Potassium
                52610	, -- Potassium
                50822	, -- Potassium, Whole Blood
                51992	, -- Protein
                50983	, -- Sodium
                52623	, -- Sodium
                51102	, -- Total Protein, Urine
                50998	, -- Transferrin
                51000	, -- Triglycerides
                -- 52642	, -- Troponin I
                51006	, -- Urea Nitrogen
                51007	-- Uric Acid


"""
def CKD_LAB():#轉檔
    try:
        
        print(f"處理表格:z_k_lab")
        with ENG.connect() as connection: 
            connection.execute(text("DELETE FROM z_k_lab"))
            

            SQL = f"""
            
            INSERT INTO z_k_lab (
                itemid,
                lab_name,
                valuenum,
                subject_id,
                hadm_id,
                charttime,
                fluid,
                category,
                ref_range_lower,
                ref_range_upper,
                flag,
                comments,hk
            )
            SELECT  
                a.itemid,
                c.label AS lab_name,
                ROUND(a.valuenum, 2) AS valuenum,
                a.subject_id,
                a.hadm_id,
                a.charttime,
                c.fluid,
                c.category,
                a.ref_range_lower,
                a.ref_range_upper,
                a.flag,
                a.comments
                , case when a.charttime < b.hk_datetime then b.potassium else '0' end as hk
            FROM z_k_adm b
            JOIN labevents a 
                ON a.subject_id = b.subject_id 
            AND a.hadm_id    = b.hadm_id
            JOIN d_labitems c 
                ON c.itemid = a.itemid
            WHERE a.itemid IN (  {LAB_IN}    )
             
        
            -- AND a.charttime >= b.admittime + INTERVAL 24 HOUR
            -- AND a.charttime <  b.dischtime + INTERVAL 72 HOUR
            AND a.valuenum REGEXP '^[0-9]+(\\.[0-9]+)?$'
            AND a.valuenum != 0
             AND a.valuenum is not NULL 
            ;
            
            """
            connection.execute(text(SQL))
            connection.commit()

            my_list = ['HEART RATE','RESPIRATORY RATE','SPO2','TEMPERATURE CELSIUS','NON INVASIVE BLOOD PRESSURE SYSTOLIC','NON INVASIVE BLOOD PRESSURE DIASTOLIC','MEAN ARTERIAL PRESSURE (NIBP)']
            #for item in my_list:

            SQL = f"""
            
            INSERT INTO z_k_lab (
                itemid,
                lab_name,
                valuenum,
                subject_id,
                hadm_id,
                charttime,
                fluid,
                category,
                hk
            )
            SELECT ce.itemid, UPPER(di.label) AS label,valuenum,w.subject_id, ce.hadm_id,charttime
                ,'' as fluid,'' as category
                -- w.potassium as hk
                , case when ce.charttime < w.hk_datetime then w.potassium else '0' end as hk
              

                    
                FROM chartevents ce
                JOIN d_items di ON di.itemid = ce.itemid
                JOIN z_k_adm   w    ON w.hadm_id = ce.hadm_id
                WHERE ce.valuenum IS NOT NULL
                AND ce.charttime >= w.admittime + INTERVAL 24 HOUR
                AND ce.charttime <  w.dischtime + INTERVAL 72 HOUR
                AND UPPER(di.label) = 'HEART RATE'
            ;
            
            """
            connection.execute(text(SQL))
            connection.commit()

            SQL = f"""
            -- 轉檔血壓
            
            INSERT INTO z_k_lab (
                itemid,
                lab_name,
                valuenum,
                subject_id,
                hadm_id,
                charttime,
                fluid,
                category,
                hk
            )
            SELECT ce.itemid
   
                , (case when UPPER(di.label) like '%SYSTOLIC%' THEN'SYSTOLIC' -- 收縮
                when UPPER(di.label) like '%DIASTOLIC%' THEN'DIASTOLIC'  -- 舒張
                end)
                AS label
                
                
                ,valuenum,w.subject_id, ce.hadm_id,charttime
                ,'' as fluid,'' as category
                -- w.potassium as hk
                , case when ce.charttime < w.hk_datetime then w.potassium else '0' end as hk
              

                    
                FROM chartevents ce
                JOIN d_items di ON di.itemid = ce.itemid
                JOIN z_k_adm   w    ON w.hadm_id = ce.hadm_id
                WHERE ce.valuenum IS NOT NULL
                AND ce.charttime >= w.admittime + INTERVAL 24 HOUR
                AND ce.charttime <  w.dischtime + INTERVAL 72 HOUR
                AND UPPER(di.label) in('NON INVASIVE BLOOD PRESSURE SYSTOLIC','NON INVASIVE BLOOD PRESSURE DIASTOLIC')
            ;
            
            """
            connection.execute(text(SQL))
            connection.commit()



            SQL = f"""
            -- 轉檔EGFR

            INSERT INTO z_k_lab (
                itemid,
                lab_name,
                valuenum,
                subject_id,
                hadm_id,
                charttime,hk
                
                
            )
              /*
                SELECT 1 AS itemid,'MDRD_EGFR' AS lab_name

                
                , ROUND(
                    175 *
                    POW(a.valuenum, -1.154) *
                    POW(b.age_years, -0.203) *
                    CASE 
                        WHEN b.gender = 'F' THEN 0.742
                        ELSE 1
                    END
                ,2) AS valuenum,

                a.subject_id,
                a.hadm_id,
                a.charttime
                -- ,b.potassium as hk
                   , case when a.charttime < b.hk_datetime then b.potassium else '0' end as hk
              

            
            FROM z_k_lab  a
            JOIN z_k_adm  b  ON (a.subject_id = b.subject_id and a.hadm_id = b.hadm_id )
            

            WHERE a.itemid = 50912
            AND a.valuenum IS NOT NULL
            AND a.valuenum > 0
            AND b.age_years IS NOT NULL
              */
              SELECT 
                2 AS itemid,
                'CKD_EPI_EGFR' AS lab_name,

                ROUND(
                    142 *
                    POWER(
                        a.valuenum / CASE WHEN b.gender = 'F' THEN 0.7 ELSE 0.9 END,
                        CASE 
                            WHEN b.gender = 'F' AND a.valuenum <= 0.7 THEN -0.241
                            WHEN b.gender = 'F' AND a.valuenum >  0.7 THEN -1.200
                            WHEN b.gender <> 'F' AND a.valuenum <= 0.9 THEN -0.302
                            ELSE -1.200
                        END
                    ) *
                    POWER(0.9938, b.age_years) *
                    CASE WHEN b.gender = 'F' THEN 1.012 ELSE 1 END
                , 2) AS valuenum,

                a.subject_id,
                a.hadm_id,
                a.charttime,

                CASE 
                    WHEN a.charttime < b.hk_datetime THEN b.potassium 
                    ELSE 0 
                END AS hk

            FROM z_k_lab a
            JOIN z_k_adm b 
                ON a.subject_id = b.subject_id 
            AND a.hadm_id = b.hadm_id

            WHERE a.itemid = 50912       -- Creatinine
            AND a.valuenum IS NOT NULL
            AND a.valuenum > 0
            AND b.age_years IS NOT NULL;
           

            """
            connection.execute(text(SQL))
            connection.commit()





     
        SQL = f"""
        

        SELECT ce.itemid,ce.subject_id,ce.hadm_id, ce.charttime
        , case ce.itemid when 220179 THEN'SYSTOLIC(SBP)'
        when 220180 THEN'DIASTOLIC(DBP)' else UPPER(di.label) END
        as lab_name
        
        , ce.valuenum
        FROM chartevents ce
        JOIN d_items di ON di.itemid = ce.itemid
        JOIN z_k_adm   s ON s.hadm_id = ce.hadm_id -- and s.AKI ='1'
        WHERE ce.charttime >= s.admittime  AND ce.charttime < s.dischtime 
        AND ce.itemid IN ('220045','220179','220180','220210','223762')
        AND ce.valuenum IS NOT NULL
        AND ce.valuenum REGEXP '^[0-9]+$' AND CAST(ce.valuenum AS UNSIGNED) > 1
        
        
        """

        #reader=pd.read_sql(text(SQL), ENG,chunksize=100000  )
            
        #for chunk in tqdm(reader):
            
            #chunk.to_sql(lab_table, con=ENG, if_exists='append', index=False, method='multi')
                    
        
        
        
        print("轉檔完成")
        return 0
    except Exception as Error:
        print("CSV_TO_DB 錯誤!!!!!!! \n"+str(Error))

def CKD_TO_STAT_EXCEL():#統計轉EXCEL
    try:
        #SQL_MAP={  "z_ckd_adm":"z_ckd_lab","z_k_adm":"z_k_lab" ,"z_sodium_adm":"z_sodium_lab" ,"z_aki_adm":"z_aki_lab"   }
        SQL_MAP={"z_k_adm":"z_k_lab"   }
        # 根據表格名稱設定工作表名稱
        sheet_names = {
            #"z_ckd_adm": "慢性腎臟病統計",
            "z_k_adm": "高血鉀統計", 
            #"z_sodium_adm": "低血鈉統計"
        }
        
        # 創建 Excel 檔案路徑
        excel_file = "EXCEL/統計資料匯總.xlsx"
        
        # 收集所有統計資料
        all_data = {}
        
        for adm_table,lab_table in SQL_MAP.items():
            SQL = f"""
            
            select
                a.itemid,
                a.lab_name                          AS 實驗室項目,
                a.fluid,
                a.category,
                COUNT(a.lab_name  ) as 筆數 ,
                round(MIN(a.valuenum)  ,2)                     AS 最小值,
                round( MAX(a.valuenum) ,2)                        AS 最大值,
                round( AVG(a.valuenum),2)                     AS 平均值,
                
            
                (    SELECT 
            round(AVG(valuenum),2) AS median_value
            FROM (
            SELECT 
                valuenum,
                ROW_NUMBER() OVER (ORDER BY valuenum) AS row_num,
                COUNT(*) OVER () AS total_rows
            FROM {lab_table} where itemid=a.itemid
            ) AS ordered
            WHERE row_num IN (FLOOR((total_rows + 1) / 2), CEIL((total_rows + 1) / 2))) as 中位數,
                round(STDDEV(a.valuenum) ,2)                  AS 標準差
            FROM
                {lab_table} a
                GROUP by a.lab_name ,a.itemid,a.fluid,a.category
                -- order by a.lab_name asc
                UNION
                select  '' as itemid ,
                'EGFR(入院)'                          AS 實驗室項目,
                '' as fluid,
                '' as category,
                COUNT(a.admit_egfr  ) as 筆數 ,
                MIN(a.admit_egfr)                     AS 最小值,
                MAX(a.admit_egfr)                     AS 最大值,
                round( AVG(a.admit_egfr),2)                     AS 平均值,
                
            
                    (    SELECT 
            AVG(admit_egfr) AS median_value
            FROM (
            SELECT 
                admit_egfr,
                ROW_NUMBER() OVER (ORDER BY admit_egfr) AS row_num,
                COUNT(*) OVER () AS total_rows
            FROM {adm_table} where {adm_table}.admit_egfr is not null
            ) AS ordered
            WHERE row_num IN (FLOOR((total_rows + 1) / 2), CEIL((total_rows + 1) / 2))) as 中位數,
                    round(STDDEV(a.admit_egfr) ,2)                  AS 標準差
                from {adm_table} a
                where a.admit_egfr is not null
            UNION
                select  '' as itemid ,
                'EGFR(最新)'                          AS 實驗室項目,
                '' as fluid,
                '' as category,
                COUNT(a.EGFR  ) as 筆數 ,
                MIN(a.EGFR)                     AS 最小值,
                MAX(a.EGFR)                     AS 最大值,
                round( AVG(a.EGFR),2)                     AS 平均值,
                
            
                    (    SELECT 
            AVG(EGFR) AS median_value
            FROM (
            SELECT 
                EGFR,
                ROW_NUMBER() OVER (ORDER BY EGFR) AS row_num,
                COUNT(*) OVER () AS total_rows
            FROM {adm_table} where {adm_table}.EGFR is not null
            ) AS ordered
            WHERE row_num IN (FLOOR((total_rows + 1) / 2), CEIL((total_rows + 1) / 2))) as 中位數,
                    round(STDDEV(a.EGFR) ,2)                  AS 標準差
                from {adm_table} a
                where a.EGFR is not null
                UNION
                select  '' as itemid ,
                'age_years'                          AS 實驗室項目,
                '' as fluid,
                '' as category,
                COUNT(a.age_years  ) as 筆數 ,
                MIN(a.age_years)                     AS 最小值,
                MAX(a.age_years)                     AS 最大值,
                round( AVG(a.age_years),2)                     AS 平均值,
                
            
                    (    SELECT 
            AVG(age_years) AS median_value
            FROM (
            SELECT 
                age_years,
                ROW_NUMBER() OVER (ORDER BY age_years) AS row_num,
                COUNT(*) OVER () AS total_rows
            FROM {adm_table} where {adm_table}.age_years is not null
            ) AS ordered
            WHERE row_num IN (FLOOR((total_rows + 1) / 2), CEIL((total_rows + 1) / 2))) as 中位數,
                    round(STDDEV(a.age_years) ,2)                  AS 標準差
                from {adm_table} a
                where a.age_years is not null
                UNION
                select  '' as itemid ,
                'bmi'                          AS 實驗室項目,
                '' as fluid,
                '' as category,
                COUNT(a.bmi  ) as 筆數 ,
                MIN(a.bmi)                     AS 最小值,
                MAX(a.bmi)                     AS 最大值,
                round( AVG(a.bmi),2)                     AS 平均值,
                
            
                    (    SELECT 
            round(AVG(bmi),2) AS median_value
            FROM (
            SELECT 
                bmi,
                ROW_NUMBER() OVER (ORDER BY bmi) AS row_num,
                COUNT(*) OVER () AS total_rows
            FROM {adm_table} where {adm_table}.bmi is not null
            ) AS ordered
            WHERE row_num IN (FLOOR((total_rows + 1) / 2), CEIL((total_rows + 1) / 2))) as 中位數,
                    round(STDDEV(a.bmi) ,2)                  AS 標準差
                from {adm_table} a
                where a.bmi is not null
        

            """
            
            print(f"處理表格:{adm_table}")
            stat_data = pd.read_sql(text(SQL), ENG)
            sheet_name = sheet_names.get(adm_table, adm_table)
            all_data[sheet_name] = stat_data
            
        # 一次性寫入所有工作表到同一個 Excel 檔案
        try:
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                for sheet_name, data in all_data.items():
                    data.to_excel(writer, sheet_name=sheet_name, index=False)
                    print(f"統計資料已儲存到工作表: {sheet_name}")
        except ImportError:
            # 如果沒有 openpyxl，使用 xlsxwriter
            print("警告: 未安裝 openpyxl，使用 xlsxwriter 引擎")
            with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
                for sheet_name, data in all_data.items():
                    data.to_excel(writer, sheet_name=sheet_name, index=False)
                    print(f"統計資料已儲存到工作表: {sheet_name}")
        
        print(f"所有統計資料已匯總到 {excel_file}")
        return 0
    except Exception as Error:
        print("CKD_TO_STAT_EXCEL 錯誤!!!!!!! \n"+str(Error))


def adm_k():#轉檔
    try:
        #         select subject_id , hadm_id as "stay_id"
        # , admittime "t0",dischtime  "t_end"
        # ,potassium  "y_hk" from z_k_adm zka 

        with ENG.connect() as connection: 
            
            connection.execute(text("DELETE FROM z_k_adm"))
            connection.commit()
            
            SQL = f"""

                INSERT INTO mimic3_1.z_k_adm
                (subject_id, hadm_id, admittime, dischtime,deathtime, admission_type, admit_provider_id, admission_location
                , insurance, marital_status, race, edregtime, edouttime, hospital_expire_flag, icd_1, icd_2, icd_3
            , age_years, gender,  potassium, body_weight,body_height)

            
                
                    
            select
            `a`.`subject_id` as `subject_id`,
            `a`.`hadm_id` as `hadm_id`,
            `a`.`admittime` as `admittime`,
            `a`.`dischtime` as `dischtime`,
            `a`.`deathtime` as `deathtime`,
            `a`.`admission_type` as `admission_type`,
            `a`.`admit_provider_id` as `admit_provider_id`,
            `a`.`admission_location` as `admission_location`,
            -- `a`.`discharge_location` as `discharge_location`,
            `a`.`insurance` as `insurance`,
            -- `a`.`language` as `language`,
            `a`.`marital_status` as `marital_status`,
            `a`.`race` as `race`,
            `a`.`edregtime` as `edregtime`,
            `a`.`edouttime` as `edouttime`,
            `a`.`hospital_expire_flag` as `hospital_expire_flag`,
            (
            select
                (case
                    when regexp_like(`diagnoses_icd`.`icd_code`, '^250|^E10|^E11|^E13') then 'Diabetes'
                    when regexp_like(`diagnoses_icd`.`icd_code`, '^401|^405|^I10|^I15|^410|^411|^414|^428|^4292|^440|^4439|^I21|^I25|^I50|^I70') then 'Cardiovascular'
                    when regexp_like(`diagnoses_icd`.`icd_code`, '^2777|^272|^E88.81|^E78|^278|^E66') then 'Metabolic'
                    else `diagnoses_icd`.`icd_code`
                end)
            from
                `diagnoses_icd`
            where
                ((`diagnoses_icd`.`subject_id` = `a`.`subject_id`)
                    and (`diagnoses_icd`.`hadm_id` = `a`.`hadm_id`)
                        and (`diagnoses_icd`.`seq_num` = 1))) as `ICD_1`,
            (
            select
                (case
                    when regexp_like(`diagnoses_icd`.`icd_code`, '^250|^E10|^E11|^E13') then 'Diabetes'
                    when regexp_like(`diagnoses_icd`.`icd_code`, '^401|^405|^I10|^I15|^410|^411|^414|^428|^4292|^440|^4439|^I21|^I25|^I50|^I70') then 'Cardiovascular'
                    when regexp_like(`diagnoses_icd`.`icd_code`, '^2777|^272|^E88.81|^E78|^278|^E66') then 'Metabolic'
                    else `diagnoses_icd`.`icd_code`
                end)
            from
                `diagnoses_icd`
            where
                ((`diagnoses_icd`.`subject_id` = `a`.`subject_id`)
                    and (`diagnoses_icd`.`hadm_id` = `a`.`hadm_id`)
                        and (`diagnoses_icd`.`seq_num` = 2))) as `ICD_2`,
            (
            select
                (case
                    when regexp_like(`diagnoses_icd`.`icd_code`, '^250|^E10|^E11|^E13') then 'Diabetes'
                    when regexp_like(`diagnoses_icd`.`icd_code`, '^401|^405|^I10|^I15|^410|^411|^414|^428|^4292|^440|^4439|^I21|^I25|^I50|^I70') then 'Cardiovascular'
                    when regexp_like(`diagnoses_icd`.`icd_code`, '^2777|^272|^E88.81|^E78|^278|^E66') then 'Metabolic'
                    else `diagnoses_icd`.`icd_code`
                end)
            from
                `diagnoses_icd`
            where
                ((`diagnoses_icd`.`subject_id` = `a`.`subject_id`)
                    and (`diagnoses_icd`.`hadm_id` = `a`.`hadm_id`)
                        and (`diagnoses_icd`.`seq_num` = 3))) as `ICD_3`,
            ((`patients`.`anchor_age` + year(`a`.`admittime`)) - `patients`.`anchor_year`) as `age_years`,
            `patients`.`gender` as `gender`,
            
            (
            select
                (case
                    when (count(0) > 0) then '1'
                    else 0
                end)
            from
                `labevents` `lab`
            where
                ((`lab`.`subject_id` = `a`.`subject_id`)
                    and (`lab`.`hadm_id` = `a`.`hadm_id`)
                   --  AND lab.charttime >= a.admittime
                    
                     AND lab.charttime >= a.admittime + INTERVAL 24 HOUR
                     AND lab.charttime <  a.admittime + INTERVAL 72 HOUR


                        and (`lab`.`itemid` in (50971,52610, 50822))
                         --   and regexp_like(`lab`.`valuenum`, '^[0-9]+(\\.[0-9]+)?$')
                                and (`lab`.`valuenum` >= 5.5))) as `POTASSIUM`
            
            ,(select
                ce.valuenum 
                    
                FROM mimic3_1.chartevents ce
                -- JOIN mimic3_1.d_items di ON di.itemid = ce.itemid
                where ce.subject_id=a.subject_id and ce.hadm_id =a.hadm_id  and  ce.itemid in('224639','226512')
                    AND ce.valuenum IS NOT NULL
                    -- 合理範圍過濾，避免錄入錯誤
                
                    LIMIT 1
                    
                    ) as body_weight,
                    (
                    select
                ce.valuenum 
                    
                FROM mimic3_1.chartevents ce
                -- JOIN mimic3_1.d_items di ON di.itemid = ce.itemid
                where ce.subject_id=a.subject_id and ce.hadm_id =a.hadm_id  and  ce.itemid in('226730')
                    AND ce.valuenum IS NOT NULL
                    -- 合理範圍過濾，避免錄入錯誤
                
                    LIMIT 1
                    
                    )as body_height
                    
            from
                `admissions` `a`
            join `patients` on
                ((`patients`.`subject_id` = `a`.`subject_id` AND`patients`.`anchor_year_group` in ('2020 - 2022', '2017 - 2019') ))
              --   ((`patients`.`subject_id` = `a`.`subject_id` AND`patients`.`anchor_year_group` in ('2020 - 2022') ))
            where 

            exists(
                         select  1 from `diagnoses_icd` where `diagnoses_icd`.`subject_id` = `a`.`subject_id`
                              
                                    and `diagnoses_icd`.`hadm_id` = `a`.`hadm_id`
                                    and `diagnoses_icd`.`seq_num` 
                                     in (1, 2, 3, 4, 5,6,7,8,9,10)
                                    and    `diagnoses_icd`.`icd_version` = 10
                                     AND (SUBSTRING(replace(`diagnoses_icd`.`icd_code`, '.', ''),1,4)) NOT in('N186','Z992','Z490','Z491','Z492','Z940','T861')
                                         
                                    and (SUBSTRING(replace(`diagnoses_icd`.`icd_code`, '.', ''),1,4)
                                     in( 'N181','N182','N183','N184','N185','N186','N189'  )
                                         )
                                         AND (
                   
                   replace(`diagnoses_icd`.`icd_code`, '.', '') like'N17%'
                  OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'N18.1%'
                   OR  replace(`diagnoses_icd`.`icd_code`, '.', '') like'N18.2%'
                   OR  replace(`diagnoses_icd`.`icd_code`, '.', '') like'N18.30%'
                   OR  replace(`diagnoses_icd`.`icd_code`, '.', '') like'N18.31%'
                   OR  replace(`diagnoses_icd`.`icd_code`, '.', '') like'N18.31%'
                   OR  replace(`diagnoses_icd`.`icd_code`, '.', '') like'N18.4%'
                    OR  replace(`diagnoses_icd`.`icd_code`, '.', '') like'N18.5%'
                     OR  replace(`diagnoses_icd`.`icd_code`, '.', '') like'N18.6%'
                      OR  replace(`diagnoses_icd`.`icd_code`, '.', '') like'N18.9%'
                   OR  replace(`diagnoses_icd`.`icd_code`, '.', '') like'E10%'
                    OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'E11%'
                    OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'E13%'
                    OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'N17%'
                    OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'I10%'
                    OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'I12%'
                    OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'I13%'
                    OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'I50%'
                    OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'I25%'
                    OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'E87.2%'
                     OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'A40%'
                     OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'A41%'
                    OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'M62.82%'
                     OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'E27.1%'
                     OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'E27.4%'
                               )              
                               
                 )
             and patients.anchor_age >=18


               
                    
            
                AND    a.hadm_id IN (
                SELECT l.hadm_id
                FROM labevents l
                WHERE l.subject_id =a.subject_id and l.hadm_id = a.hadm_id and l.itemid IN (
                   {LAB_IN}
                )
                GROUP BY l.hadm_id
                HAVING COUNT(DISTINCT l.itemid) >= 6
            )
            
            /*and  a.hadm_id IN (
            
                select hadm_id from prescriptions p
                WHERE p.subject_id =a.subject_id and p.hadm_id = a.hadm_id 
                and (LOWER(p.drug) like '%lisinopril%'
            or LOWER(p.drug) like '%enalapril%'
            or LOWER(p.drug) like '%ramipril%'
            or LOWER(p.drug) like '%losartan%'
            or LOWER(p.drug) like '%valsartan%'
            or LOWER(p.drug) like '%irbesartan%'
            or LOWER(p.drug) like '%spironolactone%'
            or LOWER(p.drug) like '%eplerenone%'
            or LOWER(p.drug) like '%amiloride%'
            or LOWER(p.drug) like '%triamterene%'

            or LOWER(p.drug) like '%heparin%'
            or LOWER(p.drug) like '%enoxaparin%'
            or LOWER(p.drug) like '%ibuprofen%'
            or LOWER(p.drug) like '%ketorolac%'
            or LOWER(p.drug) like '%furosemide%'
            or LOWER(p.drug) like '%bumetanide%'
            or LOWER(p.drug) like '%torsemide%'
            or LOWER(p.drug) like '%hydrochlorothiazide%'
            or LOWER(p.drug) like '%chlorthalidone%'
            )
                GROUP BY p.hadm_id
                HAVING COUNT(DISTINCT p.drug) >= 1
                
            
            )
            */
            
            
            
            

            
            """
            connection.execute(text(SQL))
            connection.commit()
            print(f"z_k_adm 轉檔完成")
            SQL=f"""  select a.subject_id,a.hadm_id from z_k_adm  a where exists(
                    select * from labevents b where  b.subject_id= a.subject_id 
                    and `b`.`hadm_id` = `a`.`hadm_id` AND b.itemid = 50912 
                    AND b.valuenum IS NOT null
                    )
            """
            rtn=connection.execute(text(SQL))
            results2 = rtn.mappings().all()

            SQL=f"""-- 更新BMI
            update z_k_adm set 
            bmi = ROUND(body_weight / POW(body_height / 100, 2), 2)
            WHERE body_height IS NOT NULL AND body_weight IS NOT null
            """

            connection.execute(text(SQL))

            # for I in results2:
            #     SQL=f"""

            #     WITH
            #     egfr  AS (
            #     SELECT
            #         a.subject_id, a.hadm_id ,
            #         ( select b.valuenum 
                
            #     from labevents b    
            #     where b.subject_id= a.subject_id and `b`.`hadm_id` = `a`.`hadm_id` AND itemid = 50912 AND b.valuenum IS NOT null
            #     ORDER by b.charttime DESC LIMIT 1
            #     ) as lab
                    
            #         ,a.gender, a.age_years,
            #         /* κ, α 依性別 */
            #         CASE WHEN a.gender='F' THEN 0.7 ELSE 0.9 END AS kappa,
            #         CASE WHEN a.gender='F' THEN -0.241 ELSE -0.302 END AS alpha
            #     FROM z_k_adm a  where a.subject_id='{I["subject_id"]}' AND hadm_id='{I["hadm_id"]}'
            #     )
                
            #     SELECT
            #         subject_id, hadm_id,  lab, gender, age_years,
            #         ROUND(
            #         142
            #         * POW(LEAST(lab / kappa, 1), alpha)
            #         * POW(GREATEST(lab / kappa, 1), -1.200)
            #         * POW(0.9938, age_years)
            #         * CASE WHEN gender='F' THEN 1.012 ELSE 1.000 END
            #         ,1) AS egfr_data
            #     FROM egfr

            #     """
            #     rtn=connection.execute(text(SQL))
            #     egfr_data = rtn.mappings().all()

            #     SQL=f"""

            #     WITH
            #     egfr2  AS (
            #     SELECT
            #         a.subject_id, a.hadm_id ,
            #         ( select b.valuenum 
                
            #     from labevents b    
            #     where b.subject_id= a.subject_id and `b`.`hadm_id` = `a`.`hadm_id` AND itemid = 50912 AND b.valuenum IS NOT null
            #     ORDER by b.charttime ASC LIMIT 1
            #     ) as lab
                    
            #         ,a.gender, a.age_years,
            #         /* κ, α 依性別 */
            #         CASE WHEN a.gender='F' THEN 0.7 ELSE 0.9 END AS kappa,
            #         CASE WHEN a.gender='F' THEN -0.241 ELSE -0.302 END AS alpha
            #     FROM z_k_adm a  where a.subject_id='{I["subject_id"]}' AND hadm_id='{I["hadm_id"]}'
            #     )
                
            #     SELECT
            #         subject_id, hadm_id,  lab, gender, age_years,
            #         ROUND(
            #         142
            #         * POW(LEAST(lab / kappa, 1), alpha)
            #         * POW(GREATEST(lab / kappa, 1), -1.200)
            #         * POW(0.9938, age_years)
            #         * CASE WHEN gender='F' THEN 1.012 ELSE 1.000 END
            #         ,1) AS admit_egfr
            #     FROM egfr2

            #     """
            #     rtn=connection.execute(text(SQL))
            #     admit_egfr = rtn.mappings().all()


            #     if egfr_data.__len__():

            #         SQL=f"""
            #         UPDATE z_k_adm SET
            #         EGFR='{egfr_data[0]["egfr_data"]}'
            #         ,admit_egfr='{admit_egfr[0]["admit_egfr"]}'
            #         WHERE subject_id='{I["subject_id"]}' AND hadm_id='{I["hadm_id"]}'
            #         """
            #         connection.execute(text(SQL))

            #         print(I)
            # connection.commit()
            


            #print(table_name+",完成")
        print("轉檔完成")
        return 0
    except Exception as Error:
        print("adm_k 錯誤!!!!!!! \n"+str(Error))

def adm_k3():#轉檔
    try:
        #         select subject_id , hadm_id as "stay_id"
        # , admittime "t0",dischtime  "t_end"
        # ,potassium  "y_hk" from z_k_adm zka 

        with ENG.connect() as connection: 
            
            connection.execute(text("DELETE FROM z_k_adm"))
            connection.commit()
            
            SQL = f"""

                INSERT INTO mimic3_1.z_k_adm
                (subject_id, hadm_id, admittime, dischtime,deathtime, admission_type, admit_provider_id, admission_location
                , insurance, marital_status, race, edregtime, edouttime, hospital_expire_flag
            , age_years, gender,  potassium,hk_datetime, body_weight,body_height)
               
           
            select
            `a`.`subject_id` as `subject_id`,
            `a`.`hadm_id` as `hadm_id`,
            `a`.`admittime` as `admittime`,
            `a`.`dischtime` as `dischtime`,
            `a`.`deathtime` as `deathtime`,
            `a`.`admission_type` as `admission_type`,
            `a`.`admit_provider_id` as `admit_provider_id`,
            `a`.`admission_location` as `admission_location`,
            -- `a`.`discharge_location` as `discharge_location`,
            `a`.`insurance` as `insurance`,
            -- `a`.`language` as `language`,
            `a`.`marital_status` as `marital_status`,
            `a`.`race` as `race`,
            `a`.`edregtime` as `edregtime`,
            `a`.`edouttime` as `edouttime`,
            `a`.`hospital_expire_flag` as `hospital_expire_flag`,
            
            ((`patients`.`anchor_age` + year(`a`.`admittime`)) - `patients`.`anchor_year`) as `age_years`,
            `patients`.`gender` as `gender`,
            
            (
            select
                (case
                    when (count(0) > 0) then '1'
                    else 0
                end)
            from
                `labevents` `lab`
            where
                ((`lab`.`subject_id` = `a`.`subject_id`)
                    and (`lab`.`hadm_id` = `a`.`hadm_id`)
                   --  AND lab.charttime >= a.admittime
                    
                     AND lab.charttime >= a.admittime + INTERVAL 24 HOUR
                     AND lab.charttime <  a.admittime + INTERVAL 72 HOUR


                        and (`lab`.`itemid` in (50971,52610, 50822, 52452))
                         --   and regexp_like(`lab`.`valuenum`, '^[0-9]+(\\.[0-9]+)?$')
                                and (`lab`.`valuenum` >= 5.5))) as `POTASSIUM`
             ,(
            select
                lab.charttime
            from
                `labevents` `lab`
            where
                ((`lab`.`subject_id` = `a`.`subject_id`)
                    and (`lab`.`hadm_id` = `a`.`hadm_id`)
                   --  AND lab.charttime >= a.admittime
                    
                     AND lab.charttime >= a.admittime + INTERVAL 24 HOUR
                     AND lab.charttime <  a.admittime + INTERVAL 72 HOUR


                        and (`lab`.`itemid` in (50971,52610, 50822, 52452))
                         --   and regexp_like(`lab`.`valuenum`, '^[0-9]+(\\.[0-9]+)?$')
                                and (`lab`.`valuenum` >= 5.5))
                                order by charttime asc limit 1
                                ) as `hk_datetime`
            
            ,(select
                ce.valuenum 
                    
                FROM mimic3_1.chartevents ce
                -- JOIN mimic3_1.d_items di ON di.itemid = ce.itemid
                where ce.subject_id=a.subject_id and ce.hadm_id =a.hadm_id  and  ce.itemid in('224639','226512')
                    AND ce.valuenum IS NOT NULL
                    -- 合理範圍過濾，避免錄入錯誤
                
                    LIMIT 1
                    
                    ) as body_weight,
                    (
                    select
                ce.valuenum 
                    
                FROM mimic3_1.chartevents ce
                -- JOIN mimic3_1.d_items di ON di.itemid = ce.itemid
                where ce.subject_id=a.subject_id and ce.hadm_id =a.hadm_id  and  ce.itemid in('226730')
                    AND ce.valuenum IS NOT NULL
                    -- 合理範圍過濾，避免錄入錯誤
                
                    LIMIT 1
                    
                    )as body_height
                    
            from
                `admissions` `a`
            join `patients` on
                ((`patients`.`subject_id` = `a`.`subject_id` AND`patients`.`anchor_year_group` in ('2020 - 2022', '2017 - 2019') ))
              --   ((`patients`.`subject_id` = `a`.`subject_id` AND`patients`.`anchor_year_group` in ('2020 - 2022') ))
            where 
            exists(
                         select  1 from `diagnoses_icd` where `diagnoses_icd`.`subject_id` = `a`.`subject_id`
                              
                                    and `diagnoses_icd`.`hadm_id` = `a`.`hadm_id`
                                    and `diagnoses_icd`.`seq_num` 
                                     in (1, 2, 3, 4, 5,6,7,8,9,10)
                                    and    `diagnoses_icd`.`icd_version` = 10
                                     AND (SUBSTRING(replace(`diagnoses_icd`.`icd_code`, '.', ''),1,4)) NOT in('N186','Z992','Z490','Z491','Z492','Z940','T861')
                                         
                                   
                 )



             AND patients.anchor_age >=18
            AND exists (
             
              select     1  from `labevents` `lab`
           
          
               
            where
                ((`lab`.`subject_id` = `a`.`subject_id`)
                    and (`lab`.`hadm_id` = `a`.`hadm_id`)
                   --  AND lab.charttime >= a.admittime
                    
                     AND lab.charttime >= a.admittime + INTERVAL 24 HOUR
                     AND lab.charttime <  a.admittime + INTERVAL 72 HOUR


                        and (`lab`.`itemid` in (50971,52610, 50822, 52452))
                         --   and regexp_like(`lab`.`valuenum`, '^[0-9]+(\\.[0-9]+)?$')
                                and (`lab`.`valuenum` >= 5.5))
             )
              AND    a.hadm_id IN (
                SELECT l.hadm_id
                FROM labevents l
                WHERE l.subject_id =a.subject_id and l.hadm_id = a.hadm_id and l.itemid IN (
                    {LAB_IN}   
                )
                  AND l.valuenum is not NULL 
                GROUP BY l.hadm_id
                 HAVING COUNT(DISTINCT l.itemid) >= 6
                )
           
                    
            
            """
            #connection.execute(text(SQL))
            #connection.commit()
            SQL=f"""
            INSERT INTO mimic3_1.z_k_adm
                (subject_id, hadm_id, admittime, dischtime,deathtime, admission_type, admit_provider_id, admission_location
                , insurance, marital_status, race, edregtime, edouttime, hospital_expire_flag
            , age_years, gender,  potassium, body_weight,body_height)
            select
                        `a`.`subject_id` as `subject_id`,
                        `a`.`hadm_id` as `hadm_id`,
                        `a`.`admittime` as `admittime`,
                        `a`.`dischtime` as `dischtime`,
                        `a`.`deathtime` as `deathtime`,
                        `a`.`admission_type` as `admission_type`,
                        `a`.`admit_provider_id` as `admit_provider_id`,
                        `a`.`admission_location` as `admission_location`,
                        -- `a`.`discharge_location` as `discharge_location`,
                        `a`.`insurance` as `insurance`,
                        -- `a`.`language` as `language`,
                        `a`.`marital_status` as `marital_status`,
                        `a`.`race` as `race`,
                        `a`.`edregtime` as `edregtime`,
                        `a`.`edouttime` as `edouttime`,
                        `a`.`hospital_expire_flag` as `hospital_expire_flag`,
                       
                        ((`patients`.`anchor_age` + year(`a`.`admittime`)) - `patients`.`anchor_year`) as `age_years`,
                        `patients`.`gender` as `gender`,
                        
                        (
                        select
                            (case
                                when (count(0) > 0) then '1'
                                else 0
                            end)
                        from
                            `labevents` `lab`
                        where
                            ((`lab`.`subject_id` = `a`.`subject_id`)
                                and (`lab`.`hadm_id` = `a`.`hadm_id`)
                            --  AND lab.charttime >= a.admittime
                                
                                AND lab.charttime >= a.admittime + INTERVAL 24 HOUR
                                AND lab.charttime <  a.admittime + INTERVAL 72 HOUR


                                    and (`lab`.`itemid` in (50971,52610, 50822))
                                    --   and regexp_like(`lab`.`valuenum`, '^[0-9]+(\\.[0-9]+)?$')
                                            and (`lab`.`valuenum` >= 5.5))) as `POTASSIUM`
                        
                        ,(select
                            ce.valuenum 
                                
                            FROM mimic3_1.chartevents ce
                            -- JOIN mimic3_1.d_items di ON di.itemid = ce.itemid
                            where ce.subject_id=a.subject_id and ce.hadm_id =a.hadm_id  and  ce.itemid in('224639','226512')
                                AND ce.valuenum IS NOT NULL
                                -- 合理範圍過濾，避免錄入錯誤
                            
                                LIMIT 1
                                
                                ) as body_weight,
                                (
                                select
                            ce.valuenum 
                                
                            FROM mimic3_1.chartevents ce
                            -- JOIN mimic3_1.d_items di ON di.itemid = ce.itemid
                            where ce.subject_id=a.subject_id and ce.hadm_id =a.hadm_id  and  ce.itemid in('226730')
                                AND ce.valuenum IS NOT NULL
                                -- 合理範圍過濾，避免錄入錯誤
                            
                                LIMIT 1
                                
                                )as body_height
                                
                        from
                            `admissions` `a`
                        join `patients` on ((`patients`.`subject_id` = `a`.`subject_id` AND`patients`.`anchor_year_group` in ('2020 - 2022') ))
                         inner join icustays on( icustays.hadm_id =a.hadm_id  )
                         
                        --    ((`patients`.`subject_id` = `a`.`subject_id` AND`patients`.`anchor_year_group` in ('2020 - 2022') ))
                        where 

                        exists(
                         select  1 from `diagnoses_icd` where `diagnoses_icd`.`subject_id` = `a`.`subject_id`
                              
                                    and `diagnoses_icd`.`hadm_id` = `a`.`hadm_id`
                                    and `diagnoses_icd`.`seq_num` 
                                     in (1, 2, 3, 4, 5,6,7,8,9,10)
                                    and    `diagnoses_icd`.`icd_version` = 10
                                     AND (SUBSTRING(replace(`diagnoses_icd`.`icd_code`, '.', ''),1,4)) NOT in('N186','Z992','Z490','Z491','Z492','Z940','T861')
                                         
                                   
                                         AND (
                   
                                    replace(`diagnoses_icd`.`icd_code`, '.', '') like'N17%'
                                    OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'N18.1%'
                                    OR  replace(`diagnoses_icd`.`icd_code`, '.', '') like'N18.2%'
                                    OR  replace(`diagnoses_icd`.`icd_code`, '.', '') like'N18.30%'
                                    OR  replace(`diagnoses_icd`.`icd_code`, '.', '') like'N18.31%'
                                    OR  replace(`diagnoses_icd`.`icd_code`, '.', '') like'N18.31%'
                                    OR  replace(`diagnoses_icd`.`icd_code`, '.', '') like'N18.4%'
                                        OR  replace(`diagnoses_icd`.`icd_code`, '.', '') like'N18.5%'
                                        OR  replace(`diagnoses_icd`.`icd_code`, '.', '') like'N18.6%'
                                        OR  replace(`diagnoses_icd`.`icd_code`, '.', '') like'N18.9%'
                                    OR  replace(`diagnoses_icd`.`icd_code`, '.', '') like'E10%'
                                        OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'E11%'
                                        OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'E13%'
                                        OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'N17%'
                                        OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'I10%'
                                        OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'I12%'
                                        OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'I13%'
                                        OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'I50%'
                                        OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'I25%'
                                        OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'E87.2%'
                                        OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'A40%'
                                        OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'A41%'
                                        OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'M62.82%'
                                        OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'E27.1%'
                                        OR replace(`diagnoses_icd`.`icd_code`, '.', '') like'E27.4%'
                                                )              
                                                
                                    )
                        AND patients.anchor_age >=18
                    
                        
                                
                        
                            AND    a.hadm_id IN (
                            SELECT l.hadm_id
                            FROM labevents l
                            WHERE l.subject_id =a.subject_id and l.hadm_id = a.hadm_id and l.itemid IN (
                                {LAB_IN}   
                            )
                             AND l.charttime >= a.admittime + INTERVAL 24 HOUR
                            AND l.charttime <  a.admittime + INTERVAL 72 HOUR
                             AND l.valuenum is not NULL 
                            GROUP BY l.hadm_id
                            HAVING COUNT(DISTINCT l.itemid) >= 6
                        )
                        
                        -- and not exists (  select * from z_k_adm  where z_k_adm.subject_id = a.subject_id and z_k_adm.hadm_id =a.hadm_id  )
                       
                       
                        
                      
                        
            
            """
            connection.execute(text(SQL))
            connection.commit()
           

        print("z_k_adm 轉檔完成")
        return 0
    except Exception as Error:
        print("adm_k 錯誤!!!!!!! \n"+str(Error))


def adm_k_drug():#轉檔
    try:

        with ENG.connect() as connection: 
            
            connection.execute(text("DELETE FROM z_k_drug"))
            connection.commit()
            
            SQL = f"""

                --  DROP TABLE IF EXISTS z_k_drug;

                --   CREATE  TABLE z_k_drug as
                INSERT INTO z_k_drug
                (hadm_id, starttime, drug, valuenum)
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
            
            """
            SQL="""
             INSERT INTO z_k_drug
                (hadm_id, starttime, drug, valuenum,hk)
              SELECT
                a.hadm_id,
                a.starttime,

                CASE
                    -- RAASi
                    WHEN drug_lower LIKE '%lisinopril%' THEN 'RAASi'
                    WHEN drug_lower LIKE '%enalapril%' THEN 'RAASi'
                    WHEN drug_lower LIKE '%ramipril%' THEN 'RAASi'
                    WHEN drug_lower LIKE '%losartan%' THEN 'RAASi'
                    WHEN drug_lower LIKE '%valsartan%' THEN 'RAASi'
                    WHEN drug_lower LIKE '%irbesartan%' THEN 'RAASi'

                    -- Potassium Sparing
                    WHEN drug_lower LIKE '%spironolactone%' THEN 'K_SPARING'
                    WHEN drug_lower LIKE '%eplerenone%' THEN 'K_SPARING'
                    WHEN drug_lower LIKE '%amiloride%' THEN 'K_SPARING'
                    WHEN drug_lower LIKE '%triamterene%' THEN 'K_SPARING'

                    -- Heparin
                    WHEN drug_lower LIKE '%heparin%' THEN 'HEPARIN'
                    WHEN drug_lower LIKE '%enoxaparin%' THEN 'HEPARIN'

                    -- NSAID
                    WHEN drug_lower LIKE '%ibuprofen%' THEN 'NSAID'
                    WHEN drug_lower LIKE '%ketorolac%' THEN 'NSAID'

                    -- Loop Diuretics
                    WHEN drug_lower LIKE '%furosemide%' THEN 'LOOP'
                    WHEN drug_lower LIKE '%lasix%' THEN 'LOOP'
                    WHEN drug_lower LIKE '%bumetanide%' THEN 'LOOP'
                    WHEN drug_lower LIKE '%bumex%' THEN 'LOOP'
                    WHEN drug_lower LIKE '%torsemide%' THEN 'LOOP'

                    -- Thiazide
                    WHEN drug_lower LIKE '%hydrochlorothiazide%' THEN 'THIAZIDE'
                    WHEN drug_lower LIKE '%chlorthalidone%' THEN 'THIAZIDE'
                    WHEN drug_lower LIKE '%metolazone%' THEN 'THIAZIDE'

                    -- Osmotic
                    WHEN drug_lower LIKE '%mannitol%' THEN 'OSMOTIC'
                    WHEN drug_lower LIKE '%tacrolimus%' THEN 'CNI'
					WHEN drug_lower LIKE '%cyclosporine%' THEN 'CNI'
					WHEN drug_lower LIKE '%digoxin%' THEN 'DIGOXIN'
					WHEN drug_lower LIKE '%bicarbonate%' THEN 'BICARBONATE'
					WHEN drug_lower LIKE '%sodium bicarbonate%' THEN 'BICARBONATE'
                    WHEN drug_lower LIKE '%regular%' THEN 'INSULIN_REGULAR'
                    WHEN drug_lower LIKE '%lispro%' THEN 'INSULIN_RAPID'
                    WHEN drug_lower LIKE '%aspart%' THEN 'INSULIN_RAPID'
                    WHEN drug_lower LIKE '%glargine%' THEN 'INSULIN_LONG'
                    WHEN drug_lower LIKE '%detemir%' THEN 'INSULIN_LONG'
                    WHEN drug_lower LIKE '%trimethoprim%' THEN 'TMP'
                    WHEN drug_lower LIKE '%bactrim%' THEN 'TMP'
                    WHEN drug_lower LIKE '%septra%' THEN 'TMP'
                    WHEN drug_lower LIKE '%sulfamethoxazole%' THEN 'TMP'

                END AS drug,

                1 AS valuenum,b.potassium as hk

            FROM (

                SELECT
                    a.*,
                    LOWER(a.drug) AS drug_lower
                FROM prescriptions a

            ) a

            INNER JOIN z_k_adm b
                ON (
                    b.subject_id = a.subject_id
                    AND b.hadm_id = a.hadm_id
                )

            where a.starttime is not NULL
                and drug_lower REGEXP
                'lisinopril|enalapril|ramipril|losartan|valsartan|irbesartan|
                spironolactone|eplerenone|amiloride|triamterene|
                heparin|enoxaparin|
                ibuprofen|ketorolac|
                furosemide|lasix|bumetanide|bumex|torsemide|
                hydrochlorothiazide|chlorthalidone|metolazone|
                mannitol|tacrolimus|cyclosporine|digoxin|bicarbonate|sodium bicarbonate|insulin
                |trimethoprim|bactrim|septra|sulfamethoxazole
                '
                        
            """
            connection.execute(text(SQL))
            connection.commit()
             
            
        print("轉檔完成")
        return 0
    except Exception as Error:
        print("CSV_TO_DB 錯誤!!!!!!! \n"+str(Error))

def TO_CSV(TRAN_FOLDER):#轉檔
    try:
     
        with ENG.connect() as connection: 
            SQL = f"""
            select subject_id , hadm_id as "stay_id"
            , admittime "t0",dischtime  "t_end"
            ,potassium  "y_hk" , age_years as "age",gender from z_k_adm zka 
            """
            df = pd.read_sql(text(SQL), connection)
            df.to_csv(f"{TRAN_FOLDER}/adm.csv", index=False)
            print("adm 轉CSV完成")

            SQL = f"""
            
            select hadm_id  "stay_id", charttime, UPPER(lab_name)  as  "label",valuenum ,hk
            
            from z_k_lab zkl 
            """
            df = pd.read_sql(text(SQL), connection)
            df.to_csv(f"{TRAN_FOLDER}/fetch_labevents.csv", index=False)
            print("fetch_labevents_ 轉CSV完成")

            SQL = f"""
            select hadm_id  "stay_id", starttime , drug as  "label"
            , CAST(valuenum AS CHAR) as "valuenum" ,hk 
          
            from z_k_drug zkd  
            """
            df = pd.read_sql(text(SQL), connection)
            df.to_csv(f"{TRAN_FOLDER}/高血鉀用藥.csv", index=False)
            print("高血鉀用藥 轉CSV完成")
            SQL = f"""
            select   hadm_id as "stay_id",
            potassium  "y_hk" from z_k_adm zka 
            """
            df = pd.read_sql(text(SQL), connection)
            df.to_csv(f"{TRAN_FOLDER}/POTASSIUM.csv", index=False)
            print("POTASSIUM     轉CSV完成")

            SQL = f"""
           SELECT
                a.hadm_id as stay_id,
                a.charttime,
                'URINE_OUTPUT' AS label,
                a.value AS valuenum
                 ,( SELECT potassium FROM z_k_adm  where z_k_adm.hadm_id = a.hadm_id
                LIMIT 1
                
                )AS hk 
            FROM outputevents a
            JOIN z_k_adm b
            ON a.hadm_id  = b.hadm_id
            WHERE  itemid IN (
                226559, -- Foley
                226560, -- Void
                226561, -- Condom Cath
                226584, -- Ileoconduit
                226563, -- Suprapubic
                226564, -- R Nephrostomy
                226565, -- L Nephrostomy
                226567  -- Straight Cath
            )
            AND a.charttime >= b.admittime + INTERVAL 24 HOUR
            AND a.charttime <  b.dischtime + INTERVAL 48 HOUR

            AND a.value IS NOT NULL
            AND a.value > 0
            """
            df = pd.read_sql(text(SQL), connection)
            df.to_csv(f"{TRAN_FOLDER}/URINE.csv", index=False)
            print("尿量     轉CSV完成")



              
        print("轉檔完成")
        return 0
    except Exception as Error:
        print("CSV_TO_DB 錯誤!!!!!!! \n"+str(Error))

def median_iqr(x):
    #eGFR 這類資料通常可能偏態，因此用：Median (Q1, Q3) + Mann–Whitney U

    q1 = x.quantile(0.25)
    q3 = x.quantile(0.75)
    return f"{x.median():.2f} ({q1:.2f}, {q3:.2f})"


def calc_lab_pvalues2(CSV):

    results = []

    df = pd.read_csv(CSV)

    df["valuenum"] = pd.to_numeric(df["valuenum"], errors="coerce")
    df["hk"] = pd.to_numeric(df["hk"], errors="coerce")

    for lab in df["label"].dropna().unique():

        sub = df[df["label"] == lab]

        hk_group = sub[sub["hk"] == 1]["valuenum"].dropna()
        nonhk_group = sub[sub["hk"] == 0]["valuenum"].dropna()

        if len(hk_group) < 2 or len(nonhk_group) < 2:
            continue
        #Wilcoxon rank-sum test
        stat, p_value = mannwhitneyu(
            hk_group,
            nonhk_group,
            alternative="two-sided"
        )

        hk_text = median_iqr(hk_group)
        nonhk_text = median_iqr(nonhk_group)

        if p_value < 0.001:
            p_fmt = "<0.001"
        else:
            p_fmt = f"{p_value:.3f}"

        results.append({
            "項目": lab,
            "高血鉀": hk_text,
            "非高血鉀": nonhk_text,
            "p-value": p_fmt
        })

    table1 = pd.DataFrame(results)

    with pd.ExcelWriter(
        "hyperkalemia_table1.xlsx",
        engine="openpyxl"
    ) as writer:
        table1.to_excel(
            writer,
            sheet_name="Table1",
            index=False
        )

    print(table1)

    return table1
def calc_drug_pvalues():

    results = []
    df = pd.read_csv("POTA_TRAN_DATA20260429/高血鉀用藥.csv")

    for drug in df["label"].dropna().unique():

        sub = df[df["label"] == drug]

        # contingency table
        table = pd.crosstab(
            sub["label"],
            sub["hk"]
        )

        # 至少兩組
        if table.shape[1] < 2:
            continue

        chi2, p_value, dof, expected = chi2_contingency(table)

        hk_n = (sub["hk"] == 1).sum()
        nonhk_n = (sub["hk"] == 0).sum()

        total = len(sub)

        hk_pct = hk_n / total * 100
        nonhk_pct = nonhk_n / total * 100

        if p_value < 0.001:
            p_fmt = "<0.001"
        else:
            p_fmt = f"{p_value:.3f}"

        results.append({
            "藥物": drug,
            "高血鉀": f"{hk_n} ({hk_pct:.1f}%)",
            "非高血鉀": f"{nonhk_n} ({nonhk_pct:.1f}%)",
            "p-value": p_fmt
        })
    table1 = pd.DataFrame(results)

    with pd.ExcelWriter(
        "hyperkalemia_drug_table1.xlsx",
        engine="openpyxl"
    ) as writer:
        table1.to_excel(
            writer,
            sheet_name="Table1",
            index=False
        )

    return pd.DataFrame(results)

def calc_age_gender_pvalues():

    results = []
    df = pd.read_csv("POTA_TRAN_DATA20260429/adm.csv")

    # =========================
    # 2. 建立高血鉀標籤
    # =========================
    # potassium > 5.5 視為高血鉀
    hk_df = df[df["y_hk"] == 1]
    nonhk_df = df[df["y_hk"] == 0]

   # =========================
    # 3. 年齡 P-value
    # Mann-Whitney U test
    # =========================
    u_stat, age_p = mannwhitneyu(
        hk_df["age"].dropna(),
        nonhk_df["age"].dropna(),
        alternative="two-sided"
    )

    print("==== 年齡分析 ====")

    print(
        f"高血鉀: "
        f"{hk_df['age'].median():.2f} "
        f"({hk_df['age'].quantile(0.25):.2f}, "
        f"{hk_df['age'].quantile(0.75):.2f})"
    )

    print(
        f"非高血鉀: "
        f"{nonhk_df['age'].median():.2f} "
        f"({nonhk_df['age'].quantile(0.25):.2f}, "
        f"{nonhk_df['age'].quantile(0.75):.2f})"
    )

    print(f"P-value: {age_p:.6f}")

    # =========================
    # 4. 性別 P-value
    # Chi-square test
    # =========================
    gender_table = pd.crosstab(df["gender"], df["y_hk"])

    chi2, gender_p, dof, expected = chi2_contingency(gender_table)

    print("\n==== 性別分析 ====")

    print(gender_table)

    print(f"P-value: {gender_p:.6f}")

    # =========================
    # 5. 輸出論文格式
    # =========================
    male_hk = (
        (hk_df["gender"] == "M").sum() /
        len(hk_df)
    ) * 100

    male_nonhk = (
        (nonhk_df["gender"] == "M").sum() /
        len(nonhk_df)
    ) * 100

    print("\n==== Table 1 ====")

    print(
        f"AGE\t"
        f"{hk_df['age'].median():.2f} "
        f"({hk_df['age'].quantile(0.25):.2f}, "
        f"{hk_df['age'].quantile(0.75):.2f})\t"
        f"{nonhk_df['age'].median():.2f} "
        f"({nonhk_df['age'].quantile(0.25):.2f}, "
        f"{nonhk_df['age'].quantile(0.75):.2f})\t"
        f"{age_p:.3f}"
    )

    print(
        f"Male (%)\t"
        f"{male_hk:.2f}%\t"
        f"{male_nonhk:.2f}%\t"
        f"{gender_p:.3f}"
    )

    return 0

def calc_drug_pvalues():
    # =========================
    # 1. 讀取 CSV
    # =========================
    df = pd.read_csv("POTA_TRAN_DATA20260429/高血鉀用藥.csv")

    # =========================
    # 2. 去除重複
    # 同一 stay_id 同一藥物只算一次
    # =========================
    df2 = df.drop_duplicates(subset=["stay_id", "label"])

    # =========================
    # 3. 建立結果表
    # =========================
    results = []

    drug_list = sorted(df2["label"].dropna().unique())

    for drug in drug_list:

        # 是否使用該藥物
        tmp = df2.copy()

        tmp["drug_use"] = (tmp["label"] == drug).astype(int)

        # 每位病人是否使用過該藥物
        patient_drug = (
            tmp.groupby(["stay_id", "hk"])["drug_use"]
            .max()
            .reset_index()
        )

        # 建立列聯表
        table = pd.crosstab(
            patient_drug["drug_use"],
            patient_drug["hk"]
        )

        # 避免缺欄位
        table = table.reindex(
            index=[0,1],
            columns=[0,1],
            fill_value=0
        )

        # =========================
        # P-value
        # 小樣本可改 Fisher
        # =========================
        try:
            chi2, p, dof, expected = chi2_contingency(table)

            # 若期望值太小，改用 Fisher
            if (expected < 5).any():
                _, p = fisher_exact(table)

                test_name = "Fisher"

            else:
                test_name = "Chi-square"

        except:
            p = 1
            test_name = "NA"

        # =========================
        # 統計比例
        # =========================
        hk_total = table[1].sum()
        nonhk_total = table[0].sum()

        hk_drug = table.loc[1,1]
        nonhk_drug = table.loc[1,0]

        hk_rate = (hk_drug / hk_total * 100) if hk_total > 0 else 0
        nonhk_rate = (nonhk_drug / nonhk_total * 100) if nonhk_total > 0 else 0
        #p_value = float(p)
        p_text = "<0.001" if p < 0.001 else round(p, 3)
        results.append({
            "Drug": drug,
            "HK n(%)": f"{hk_drug} ({hk_rate:.1f}%)",
            "Non-HK n(%)": f"{nonhk_drug} ({nonhk_rate:.1f}%)",
            "P-value":p_text,
            "Test": test_name
        })

    # =========================
    # 4. 輸出結果
    # =========================
    result_df = pd.DataFrame(results)

    #result_df = result_df.sort_values("P-value")

    print(result_df)
    with pd.ExcelWriter(
        "drug_pvaue.xlsx",
        engine="openpyxl"
    ) as writer:
        result_df.to_excel(
            writer,
            sheet_name="Table1",
            index=False
        )


    return 0

if __name__ == "__main__":
    TRAN_FOLDER="POTA_TRAN_DATA20261014"
    #TRAN_FOLDER="real_data2"
    #calc_drug_pvalues()
    #calc_age_gender_pvalues()
    #calc_lab_pvalues2(f"{TRAN_FOLDER}/fetch_labevents.csv")

    #calc_lab_pvalues2("POTA_TRAN_DATA20260429/URINE.csv")
   
    #calc_drug_pvalues()#生成P值
    
    
    #adm_k3()
    #adm_k_drug()
   
    CKD_LAB()
    #CKD_TO_STAT_EXCEL()
    #TO_CSV(TRAN_FOLDER)



