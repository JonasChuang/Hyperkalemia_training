#MIMIC 資料庫轉檔

import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine, text

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
                comments
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
            "z_ckd_adm": "慢性腎臟病統計",
            "z_k_adm": "高血鉀統計", 
            "z_sodium_adm": "低血鈉統計"
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
                                         
                                    and (SUBSTRING(replace(`diagnoses_icd`.`icd_code`, '.', ''),1,4)
                                     in( 'E875','N181','N182','N183','N184','N185','N186','N189'  )
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


                        and (`lab`.`itemid` in (50971,52610, 50822, 52452))
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

              patients.anchor_age >=18
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
            connection.execute(text(SQL))
            connection.commit()
            SQL=f"""
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
                        --    ((`patients`.`subject_id` = `a`.`subject_id` AND`patients`.`anchor_year_group` in ('2020 - 2022') ))
                        where 

                        exists(
                                    select  1 from `diagnoses_icd` where `diagnoses_icd`.`subject_id` = `a`.`subject_id`
                                        
                                                and `diagnoses_icd`.`hadm_id` = `a`.`hadm_id`
                                                and `diagnoses_icd`.`seq_num` 
                                                in (1, 2, 3, 4, 5,6,7,8,9,10)
                                                and    `diagnoses_icd`.`icd_version` = 10
                                                    
                                                and (SUBSTRING(replace(`diagnoses_icd`.`icd_code`, '.', ''),1,4)
                                                in( 'E875','N181','N182','N183','N184','N185','N186','N189'  )
                                                    )              
                                        
                            )
                        AND patients.anchor_age >=18
                    
                        
                                
                        
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
                        
                        and not exists (
                        
                        select * from z_k_adm  where z_k_adm.subject_id = a.subject_id and z_k_adm.hadm_id =a.hadm_id
                        
                        )
            
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
            connection.execute(text(SQL))
            connection.commit()
             
            
        print("轉檔完成")
        return 0
    except Exception as Error:
        print("CSV_TO_DB 錯誤!!!!!!! \n"+str(Error))

def TO_CSV():#轉檔
    try:
     
        with ENG.connect() as connection: 
            SQL = f"""
            select subject_id , hadm_id as "stay_id"
            , admittime "t0",dischtime  "t_end"
            ,potassium  "y_hk" , age_years as "age" from z_k_adm zka 
            """
            df = pd.read_sql(text(SQL), connection)
            df.to_csv(f"POTA_TRAN_DATA20260429/adm.csv", index=False)
            print("adm 轉CSV完成")

            SQL = f"""
            
            select hadm_id  "stay_id", charttime, UPPER(lab_name)  as  "label",valuenum from z_k_lab zkl 
            """
            df = pd.read_sql(text(SQL), connection)
            df.to_csv(f"POTA_TRAN_DATA20260429/fetch_labevents.csv", index=False)
            print("fetch_labevents_ 轉CSV完成")

            SQL = f"""
            select hadm_id  "stay_id", starttime , drug as  "label", CAST(valuenum AS CHAR) as "valuenum" from z_k_drug zkd  
            """
            df = pd.read_sql(text(SQL), connection)
            df.to_csv(f"POTA_TRAN_DATA20260429/高血鉀用藥.csv", index=False)
            print("高血鉀用藥 轉CSV完成")
            SQL = f"""
            select   hadm_id as "stay_id",
            potassium  "y_hk" from z_k_adm zka 
            """
            df = pd.read_sql(text(SQL), connection)
            df.to_csv(f"POTA_TRAN_DATA20260429/POTASSIUM.csv", index=False)
            print("POTASSIUM     轉CSV完成")

            SQL = f"""
           SELECT
                a.hadm_id as stay_id,
                a.charttime,
                'URINE_OUTPUT' AS label,
                a.value AS valuenum
            FROM outputevents a
            JOIN z_k_adm b
            ON a.hadm_id  = b.hadm_id
            WHERE 
            a.charttime >= b.admittime + INTERVAL 24 HOUR
            AND a.charttime <  b.dischtime + INTERVAL 48 HOUR

            AND a.value IS NOT NULL
            AND a.value > 0
            """
            df = pd.read_sql(text(SQL), connection)
            df.to_csv(f"POTA_TRAN_DATA20260429/URINE.csv", index=False)
            print("尿量     轉CSV完成")



              
        print("轉檔完成")
        return 0
    except Exception as Error:
        print("CSV_TO_DB 錯誤!!!!!!! \n"+str(Error))

def TO_CSV2():#轉檔
    try:
     
        with ENG.connect() as connection: 
            SQL = f"""
            select distinct subject_id , stay_id 
            , admittime "t0",dischtime  "t_end"
            ,potassium  "y_hk" from z_k_adm zka 
            """
            df = pd.read_sql(text(SQL), connection)
            df.to_csv(f"POTA_TRAN_DATA20260429/adm.csv", index=False)
            print("adm 轉CSV完成")

            SQL = f"""
            
            
                select z_k_adm.stay_id , charttime, UPPER(lab_name)  as  "label",valuenum 
                from z_k_lab zkl 
                join z_k_adm on zkl.hadm_id =z_k_adm.hadm_id
                and z_k_adm.stay_id is not NuLL
            """
            df = pd.read_sql(text(SQL), connection)
            df.to_csv(f"POTA_TRAN_DATA20260429/fetch_labevents.csv", index=False)
            print("fetch_labevents_ 轉CSV完成")

            SQL = f"""
            select z_k_adm.stay_id, starttime , drug as  "label", CAST(valuenum AS CHAR) as "valuenum" 
            
            from z_k_drug zkd  
            join z_k_adm on zkd.hadm_id =z_k_adm.hadm_id
                and z_k_adm.stay_id is not NuLL


            """
            df = pd.read_sql(text(SQL), connection)
            df.to_csv(f"POTA_TRAN_DATA20260429/高血鉀用藥.csv", index=False)
            print("高血鉀用藥 轉CSV完成")
            SQL = f"""
            select   hadm_id as "stay_id",
            potassium  "y_hk" from z_k_adm zka 
            """
            df = pd.read_sql(text(SQL), connection)
            df.to_csv(f"POTA_TRAN_DATA20260429/POTASSIUM.csv", index=False)
            print("POTASSIUM     轉CSV完成")

            SQL = f"""
           SELECT
                a.hadm_id as stay_id,
                a.charttime,
                'URINE_OUTPUT' AS label,
                a.value AS valuenum
            FROM outputevents a
            JOIN z_k_adm b
            ON a.stay_id  = b.stay_id
            WHERE 
            -- a.charttime >= b.admittime + INTERVAL 24 HOUR
           -- AND a.charttime <  b.dischtime + INTERVAL 48 HOUR

            a.value IS NOT NULL
            AND a.value > 0
            """
            df = pd.read_sql(text(SQL), connection)
            df.to_csv(f"POTA_TRAN_DATA20260429/URINE.csv", index=False)
            print("尿量     轉CSV完成")





              
        print("轉檔完成")
        return 0
    except Exception as Error:
        print("CSV_TO_DB 錯誤!!!!!!! \n"+str(Error))

if __name__ == "__main__":
    
    
    adm_k3()
    adm_k_drug()
   
    CKD_LAB()
    #CKD_TO_STAT_EXCEL()
    TO_CSV()

SQL = """ 
UPDATE labevents
SET lab_name = (
    SELECT label 
    FROM d_labitems 
    WHERE d_labitems.itemid = labevents.itemid
)
WHERE lab_name IS NULL
ORDER BY row_id
LIMIT 2000000;
"""