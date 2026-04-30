   --檢驗
  SELECT ENCOUNTER_NO AS "stay_id"
  ,to_char(REPORT_DATE,'yyyy-mm-dd hh24:mi:ss') AS "charttime" ,LAB_CODE,RESULT AS "valuenum"
                ,
                CASE WHEN LAB_CODE='BUN'THEN 'UREA NITROGEN'
                WHEN LAB_CODE='CRE'THEN 'CREATININE'
                WHEN LAB_CODE='ALB'THEN 'ALBUMIN'
                WHEN LAB_CODE='HB'THEN 'Hematology'
                WHEN LAB_CODE='CA'THEN 'CALCIUM'
                WHEN LAB_CODE='P'THEN 'PHOSPHATE'
                WHEN LAB_CODE='CHOL'THEN 'CHOLESTEROL, TOTAL'
                WHEN LAB_CODE='TG'THEN 'TRIGLYCERIDES'
                WHEN LAB_CODE='LDL'THEN 'CHOLESTEROL, LDL, CALCULATED'
                WHEN LAB_CODE='HDL'THEN 'CHOLESTEROL, HDL'
                WHEN LAB_CODE='NA'THEN 'SODIUM'
                WHEN LAB_CODE='K'THEN 'POTASSIUM'
                WHEN LAB_CODE='3012036'THEN '% HEMOGLOBIN A1C'
                WHEN LAB_CODE='GLUAC'THEN 'GLUCOSE'
                WHEN LAB_CODE='UA'THEN 'URIC ACID'
              --  WHEN LAB_CODE='TP'THEN ''
               -- WHEN LAB_CODE='UPCR'THEN ''
               -- WHEN LAB_CODE='UACR'THEN ''
               
                END AS "label"
                FROM LIS.LIS_RESULT WHERE ENCOUNTER_NO in(
                 select 
                  A.ENCOUNTER_NO 
           
            from     emr.emr_ipd_encounter A
            where ADMIT_DATE >=SYSDATE -100
         
            --住院狀態     A:在院中/M:醫師准許出院/I:護理站出院程序/D:已繳費/R:自行離院/CR:取消預約住院/CA:取消住院/CD:取消結帳
            and PAT_State  in('A','M','I','D','R')
            AND BED_NO like'ICU%'
            AND EXISTS(
            
            
                SELECT * FROM LIS.LIS_RESULT WHERE LIS_RESULT.ENCOUNTER_NO =A.ENCOUNTER_NO
                AND LAB_CODE
                in('BUN','CRE','ALB','HB','CA','P','CHOL','TG','LDL','NA','K'
                ,'3012036'--HBA12C
                ,'GLUAC','UPCR','UACR','UA','UPH','4022018','4022002')
                 GROUP BY LIS_RESULT.ENCOUNTER_NO
    			HAVING COUNT(DISTINCT LIS_RESULT.LAB_CODE) > 10
            
            )
            

                
                )
                AND RESULT IS NOT NULL
                
                AND LAB_CODE
                in('BUN','CRE','ALB','HB','CA','P','CHOL','TG','LDL','NA','K'
                ,'3012036'--HBA12C
                ,'GLUAC','UPCR','UACR','UA','UPH','4022018','4022002')
                AND RESULT NOT LIKE'%>%'
                AND RESULT NOT LIKE'%<%'
                
              
              
                


 --住院病人               
                 select to_char(A.ADMIT_DATE,'yyyy-mm-dd hh24:mi:ss')as  "t0"
                   ,A.CHART_NO,A.ENCOUNTER_NO AS "stay_id"
            ,(select ICD_CODE from EMR.IER_PAT_DIAG10 t where  encounter_no=A.encounter_no and DIAG_SEQ='1' and DIAG_TYPE='I'  and CANCELED_DATE is null and rownum= 1 )as IN_ICD_CODE1
            ,(select ICD_CODE from EMR.IER_PAT_DIAG10 t where  encounter_no=A.encounter_no and DIAG_SEQ='2' and DIAG_TYPE='I'  and CANCELED_DATE is null and rownum= 1 )as IN_ICD_CODE2
            ,(select ICD_CODE from EMR.IER_PAT_DIAG10 t where  encounter_no=A.encounter_no and DIAG_SEQ='3' and DIAG_TYPE='I'  and CANCELED_DATE is null and rownum= 1 )as IN_ICD_CODE3
            ,(select ICD_CODE from EMR.IER_PAT_DIAG10 t where  encounter_no=A.encounter_no and DIAG_SEQ='1' and DIAG_TYPE='O'  and CANCELED_DATE is null and rownum= 1 )as OUT_ICD_CODE1

            ,PAT_State,BED_NO
          
        
                    ,(DEPT_CODE ||':' ||DEPT_NAME) AS DEPT
                   
          ,(
                          SELECT CASE DISCHG_REASON 
                            WHEN '0' THEN'0.其它' 
                            WHEN '1' THEN'1.依醫囑出院' 
                            WHEN '3' THEN'3.依醫囑出院改本院門診治療' 
                            WHEN '4' THEN'4.死亡' 
                            WHEN '5' THEN'5.一般自行要求出院' 
                            WHEN '6' THEN'6.安排至其他醫院' 
                            WHEN '8' THEN'8.逾假未歸或不假離院' 

                            WHEN 'A' THEN'A.病危自動出院' 
                            WHEN 'B' THEN'B.住院30日內因身分變更切帳申報後，轉為論日支付或代辦之非Tw-DRGs案件' 
                            WHEN 'D' THEN'D.醫院間轉急性後期照護' 
                            WHEN 'E' THEN'E.院內轉急性後期照護' 
                            WHEN 'F' THEN'F.因療程需要計劃性出院' 

                            WHEN 'G' THEN'G.依醫囑出院轉機構照護' 
                            WHEN 'H' THEN'H.依醫囑出院並轉介照管中心/長照需求評估' 
                            WHEN 'I' THEN'I.依醫囑出院並安排居家醫療' 
                            WHEN 'J' THEN'J.依醫囑出院並轉社區精神醫療' 

                            WHEN 'K' THEN'K.轉自費身份繼續住院' 

                            ELSE DISCHG_REASON END AS DISCHG_REASON_NAME
                            --,DISCHG_REASON


                            FROM EMR.EMR_IPD_ENCOUNTER eie  WHERE  ENCOUNTER_NO =A.ENCOUNTER_NO AND ROWNUM <= 1

                          
                          
                          
                          )AS DISCHG_REASON_NAME
                          ,  TO_CHAR(CLOSE_DATE,'yyyy-mm-dd hh24:mi:ss') as "t_end" -- 出院日期
                         
           

            ,(SELECT CASE WHEN IER_ORDER.ORDER_CODE in('58091322001','58111322002','580101322001','580111322002A','580111322002B','TJQBR01') THEN'腹膜透析' 
                WHEN IER_ORDER.ORDER_CODE in('580291323001','580011PF0001','580271PF0001','580291323001-0','580291323001-1') THEN'血液透析' 
                            ELSE'' END FROM EMR.IER_ORDER 
                WHERE  IER_ORDER.ENCOUNTER_NO = A.ENCOUNTER_NO 
                AND IER_ORDER.ORDER_CODE in('58091322001','58111322002','580101322001','580111322002A','580111322002B','TJQBR01','580291323001','580011PF0001','580271PF0001','580291323001-0','580291323001-1')
                FETCH NEXT 1 ROWS ONLY
                    ) AS HEMO --透析方式
                     ,(

                            select
                                TO_CHAR(DATE_CREATED,'yyyy-mm-dd')||':'||ORGAN_DONATE_DESC

                            from com.icc_organ_donate_rec_view
                            WHERE icc_organ_donate_rec_view.CHART_NO = A.CHART_NO
                            AND ORGAN_DONATE_FLAG IS NOT NULL

                            ) AS DNR
                           
            
            from     emr.emr_ipd_encounter A
            where ADMIT_DATE >=SYSDATE -100
         
            --住院狀態     A:在院中/M:醫師准許出院/I:護理站出院程序/D:已繳費/R:自行離院/CR:取消預約住院/CA:取消住院/CD:取消結帳
            and PAT_State  in('A','M','I','D','R')
            AND BED_NO like'ICU%'
            AND EXISTS(
            
            
                SELECT * FROM LIS.LIS_RESULT WHERE LIS_RESULT.ENCOUNTER_NO =A.ENCOUNTER_NO
                AND LAB_CODE
                in('BUN','CRE','ALB','HB','CA','P','CHOL','TG','LDL','NA','K'
                ,'3012036'--HBA12C
                ,'GLUAC','UPCR','UACR','UA','UPH','4022018','4022002')
                GROUP BY LIS_RESULT.ENCOUNTER_NO
    			HAVING COUNT(DISTINCT LIS_RESULT.LAB_CODE) >10
            
            )
            
--
--護理給藥紀錄
SELECT  ENCOUNTER_NO as "stay_id",TO_CHAR( SIGN_DATE ,'yyyy-mm-dd hh24:mi:ss') AS "starttime" 
           
,'1' as valuenum
           ,(
              CASE WHEN MED_CODE IN('KCABU01', 'KTRIT04', 'KTANA01', 'KACER01', 'KAMTR01') THEN 'ACEI'
                   WHEN MED_CODE IN('KEAZI01','KTRIC01','CFURS01','KBUDE01','KBUSI01','KFUMI01','KTORS01','KURET01') THEN 'THIAZIDE'
                   WHEN MED_CODE IN('CFURS01','KBUDE01','KBUSI01','KFUMI01','KTORS01','KURET01') THEN 'LOOP'
                   WHEN MED_CODE IN('KENTR02', 'KDIOV02', 'KIRBE01', 'KBLOP01', 'KHYZA02', 'KEXFO02', 'KSEVI02') THEN 'ARB'
                   WHEN MED_CODE IN('C03DA04') THEN 'K_SPARING'
                   WHEN MED_CODE IN('KJOLI01', 'WDICL01', 'KDEFL01', 'KACEO02', 'CLAST01', 'KMELO01', 'KIDOF01', 'KNAPR01', 'KCELE02') THEN 'NSAID'
                  -- ELSE 'OTHER' END AS drug
    END


           )AS "label",''AS LAB_CODE
           
                FROM NIS.NIS_EXECUTE_MED_RECORD nemr 
                    WHERE 
                    ENCOUNTER_NO in(
                    
                     select 
                  A.ENCOUNTER_NO 
           
            from     emr.emr_ipd_encounter A
            where ADMIT_DATE >=SYSDATE -100
         
            --住院狀態     A:在院中/M:醫師准許出院/I:護理站出院程序/D:已繳費/R:自行離院/CR:取消預約住院/CA:取消住院/CD:取消結帳
            and PAT_State  in('A','M','I','D','R')
            AND BED_NO like'ICU%'
            AND EXISTS(
            
            
                SELECT * FROM LIS.LIS_RESULT WHERE LIS_RESULT.ENCOUNTER_NO =A.ENCOUNTER_NO
                AND LAB_CODE
                in('BUN','CRE','ALB','HB','CA','P','CHOL','TG','LDL','NA','K'
                ,'3012036'--HBA12C
                ,'GLUAC','UPCR','UACR','UA')
                GROUP BY LIS_RESULT.ENCOUNTER_NO
    			HAVING COUNT(DISTINCT LIS_RESULT.LAB_CODE) >10
            
            )
            
                    )
                    AND MED_CODE in 
                    (   
                       'KCABU01', 'KTRIT04', 'KTANA01', 'KACER01', 'KAMTR01' --ACEI
                      ,'KJOLI01', 'WDICL01', 'KDEFL01', 'KACEO02', 'CLAST01', 'KMELO01', 'KIDOF01', 'KNAPR01', 'KCELE02'--NSAID
                       
                       ,'KEAZI01','KTRIC01','CFURS01','KBUDE01','KBUSI01','KFUMI01','KTORS01','KURET01'--THIAZIDE
                       
                        ,'CFURS01','KBUDE01','KBUSI01','KFUMI01','KTORS01','KURET01'--LOOP
                        
                       ,'KENTR02', 'KDIOV02', 'KIRBE01', 'KBLOP01', 'KHYZA02', 'KEXFO02', 'KSEVI02' --ARB
          				,'C03DA04'          --K_SPARING
                          
                    )
                    AND SIGN_DATE IS NOT NULL
                ORDER BY DISPLAY_NAME ASC
                
                
                
                