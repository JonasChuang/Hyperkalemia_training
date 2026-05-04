
import os, json, joblib, numpy as np, pandas as pd, shap
from typing import Dict, Tuple, List
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, average_precision_score, f1_score, brier_score_loss, precision_recall_curve,roc_curve
import tensorflow as tf
from keras import layers
import keras
import matplotlib.pyplot as plt

#from  Hyperkalemia import SEQ_HOURS

#SEQ_HOURS = Hyperkalemia.SEQ_HOURS

SEQ_HOURS = int(os.getenv("SEQ_HOURS", "72"))     # 特徵序列長度（0–24h）
LBL_FROM  = int(os.getenv("LBL_FROM_H", "24"))    # 標籤視窗起點（相對 t0 小時）
LBL_TO    = int(os.getenv("LBL_TO_H", "72"))      # 標籤視窗終點
K_CUTOFF  = float(os.getenv("K_CUTOFF", "5.5"))   # 高血鉀閾值
RANDOM_STATE = int(os.getenv("RANDOM_STATE", "42"))
TEST_SIZE = float(os.getenv("TEST_SIZE", "0.2"))
VAL_SIZE  = float(os.getenv("VAL_SIZE",  "0.2"))



# FEATURE_MAP = {
#     # 生命徵象 (Vitals)
#     'HEART RATE':'hr',
#     'RESPIRATORY RATE':'rr',
#     'SPO2':'spo2',
#     'TEMPERATURE CELSIUS':'temp',
#     'NON INVASIVE BLOOD PRESSURE SYSTOLIC':'sbp',
#     'NON INVASIVE BLOOD PRESSURE DIASTOLIC':'dbp',
#     'MEAN ARTERIAL PRESSURE (NIBP)':'map',
    
#     # 基本檢驗 (Basic Labs)
#     'SODIUM':'sodium',
#     'BICARBONATE':'bicarb',
#     'CHLORIDE':'chloride',
#     'CREATININE':'creatinine',
#     'UREA NITROGEN':'bun',
#     'GLUCOSE':'glucose',
    
#     # 額外檢驗 (Additional Labs) - 從 SQL 查詢中補充
#     'ALBUMIN':'albumin',                    # 白蛋白
#     'HEMOGLOBIN':'hemoglobin',              # 血色素
#     #'FREE CALCIUM':'calcium',               # 游離鈣
#     'PHOSPHATE':'phosphate',                # 血磷
#     'URIC ACID':'uric_acid',                # 尿酸
#     'CHOLESTEROL, TOTAL':'cholesterol',     # 總膽固醇
#     'TRIGLYCERIDES':'triglycerides',        # 三酸甘油脂
#     'CHOLESTEROL, LDL, CALCULATED':'ldl',   # 低密度膽固醇
#     '% HEMOGLOBIN A1C':'hba1c',             # 糖化血色素
#     'PROTEIN, URINE':'protein_urine',       # 蛋白尿
#     'ESTIMATED GFR (MDRD EQUATION)':'egfr', # 腎絲球過濾率
#     'PH':'pH', # 腎絲球過濾率
#     'CO2':'Calculated Total CO2',
#     'PCO2':'pCO2',
#     'TOTAL PROTEIN, URINE':'TOTAL PROTEIN, URINE',
#     #'PCO2_1':'pCO2, Body Fluid',
    
#     # 尿量 (Urine Output)
#     'URINE_OUTPUT':'urine',
#     #藥物
#     'RAASi': 'drug_raasi',
#     'K_SPARING': 'drug_k_sparing',
#     'NSAID': 'drug_nsaid',
#     'HEPARIN': 'drug_heparin',
#     'LOOP': 'drug_loop',
#     'THIAZIDE': 'drug_thiazide'
    
# }

FEATURE_MAP = {

    # ===== Vital =====
    'HEART RATE':'hr',
    'RESPIRATORY RATE':'rr',
    'SPO2':'spo2',
    'TEMPERATURE CELSIUS':'temp',
    'NON INVASIVE BLOOD PRESSURE SYSTOLIC':'sbp',
    'NON INVASIVE BLOOD PRESSURE DIASTOLIC':'dbp',
    'MEAN ARTERIAL PRESSURE (NIBP)':'map',

    # ===== Electrolyte =====
    'SODIUM':'sodium',
    'SODIUM, WHOLE BLOOD':'sodium',

    'CHLORIDE':'chloride',

    # ⚠️ potassium 已排除（正確）
    # 'POTASSIUM':'potassium',

    # ===== Renal =====
    'CREATININE':'creatinine',
    'UREA NITROGEN':'bun',
    'ESTIMATED GFR (MDRD EQUATION)':'egfr',

    # ===== Acid-base（重要🔥）=====
    'PH':'ph',
    'PH, VENOUS':'ph',

    'PCO2':'pco2',
    'PCO2, VENOUS':'pco2',

    'CALCULATED TOTAL CO2':'hco3',
    'BICARBONATE':'hco3',

    # ===== Metabolic =====
    'GLUCOSE':'glucose',

    # ===== Nutrition / protein =====
    'ALBUMIN':'albumin',
    'ALBUMIN, URINE':'albumin_urine',
    'TOTAL PROTEIN, URINE':'protein_urine',

    # ===== Hematology =====
    'HEMOGLOBIN':'hemoglobin',

    # ===== Lipid =====
    'CHOLESTEROL, TOTAL':'cholesterol',
    'CHOLESTEROL, LDL, CALCULATED':'ldl',
    'CHOLESTEROL, HDL':'hdl',
    'TRIGLYCERIDES':'triglycerides',

    # ===== Other labs =====
    'PHOSPHATE':'phosphate',
    'URIC ACID':'uric_acid',
    '% HEMOGLOBIN A1C':'hba1c',

    # ===== Urine =====
    'URINE_OUTPUT':'urine',

    # ===== Drugs =====
    'RAASI':'drug_raasi',
    'K_SPARING':'drug_k_sparing',
    'NSAID':'drug_nsaid',
    'HEPARIN':'drug_heparin',
    'LOOP':'drug_loop',
    'THIAZIDE':'drug_thiazide'
}



def to_hourly(df_long: pd.DataFrame, t0_df: pd.DataFrame) -> pd.DataFrame:
    """
    將不規則時間序列對齊到固定小時格，便於 pivot 成 N×T×F（每個病人每小時每個特徵一個值）。
    因為使用 tail(1)，排序很重要：charttime 要由早到晚，否則會取到錯誤的那一筆。
    簡單輸出範例列（意義）：

    stay_id=1001, time_index=0, label=HR, value=88 → 第 0 小時心跳 88
    stay_id=1001, time_index=1, label=SODIUM, value=137 → 第 1 小時血鈉 137
    若要改成按半小時或其它窗寬，只需改 offset 的分箱/round 邏輯即可。
    """
    if df_long.empty:
        return pd.DataFrame(columns=["stay_id","time_index","label","value"])
    df = df_long.merge(t0_df[["stay_id","t0"]], on="stay_id", how="left")

    df["offset_h"] = (pd.to_datetime(df["charttime"]) - pd.to_datetime(df["t0"])).dt.total_seconds()/3600.0
    #df["offset_h"] = (pd.to_datetime(df["charttime"]) - pd.to_datetime(df["t0"])).dt.total_seconds()/7200.0
    df = df[(df["offset_h"] >= 0) & (df["offset_h"] < SEQ_HOURS)].copy()
   # df["time_index"] = df["offset_h"].round().astype(int)
    df["time_index"] = np.floor(df["offset_h"]).astype(int)
    df.sort_values(["stay_id","label","charttime"], inplace=True)
    df = df.groupby(["stay_id","label","time_index"], as_index=False).tail(1)#同一小時若多筆只留最新值
    return df[["stay_id","label","time_index","valuenum"]].rename(columns={"valuenum":"value"})

def to_hourly_drug(df_drug: pd.DataFrame, stays: pd.DataFrame) -> pd.DataFrame:
    """
    將 prescriptions → 每小時藥物 exposure (0/1 指標)
    """
    if df_drug.empty:
        return pd.DataFrame(columns=["stay_id", "time_index", "label", "value"])

    df = df_drug.merge(stays[["stay_id", "t0"]], on="stay_id", how="left")

    df["offset_h"] = (pd.to_datetime(df["starttime"]) - pd.to_datetime(df["t0"])).dt.total_seconds()/3600.0
    df = df[(df["offset_h"] >= 0) & (df["offset_h"] < SEQ_HOURS)].copy()

    #df["time_index"] = df["offset_h"].round().astype(int)
    df["time_index"] = np.floor(df["offset_h"]).astype(int)

    # 每一小時每一類藥物取 1（有給藥就算 1）
    df["value"] = 1

    return df[["stay_id", "time_index", "label", "value"]]